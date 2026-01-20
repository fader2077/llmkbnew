import json
import re
import hashlib
from typing import List, Dict, Tuple, Any, Iterable
from pathlib import Path
from ollama import Client
from config import CONFIG, TRIPLE_PROMPT_TEMPLATE
from src.models import OllamaVectorEmbedder
from src.database import ensure_vector_index, ensure_fulltext_index, ensure_entity_index
# ✅ 從 utils.py 匯入通用工具函數
from src.utils import chunk_text, parse_triples, deduplicate_triples, normalize_text

# ✅ 預設值仍從 CONFIG 讀取，但允許覆蓋
DEFAULT_CHUNK_SIZE = CONFIG["optimal_indexing"]["chunk_size"]
DEFAULT_CHUNK_OVERLAP = CONFIG["optimal_indexing"]["overlap"]
DATASET_ID = CONFIG["infrastructure"]["dataset_id"]


def load_chunks(path: Path, chunk_size: int = None, overlap: int = None) -> List[Dict[str, str]]:
    """
    載入並切分文本
    ✅ 修正：加入 chunk_size 與 overlap 參數，支援消融實驗動態調整
    """
    if not path.exists():
        raise FileNotFoundError(f"Knowledge base not found: {path}")
    
    # 決定使用傳入參數或預設值
    size = chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE
    ovlp = overlap if overlap is not None else DEFAULT_CHUNK_OVERLAP
    
    print(f"    📄 Chunking strategy: Size={size}, Overlap={ovlp}")
    
    raw_text = path.read_text(encoding="utf-8")
    segments = chunk_text(raw_text, size, ovlp)
    
    docs: List[Dict[str, str]] = []
    for idx, segment in enumerate(segments):
        text = segment.strip()
        doc_id = f"{DATASET_ID}_chunk_{idx:05d}"
        docs.append(
            {
                "id": doc_id,
                "text": text,
                "source": path.name,
                "hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    return docs
def upsert_chunks(driver, embedder: OllamaVectorEmbedder, docs: List[Dict[str, str]]) -> Tuple[int, int]:
    inserted = 0
    skipped = 0
    with driver.session() as session:
        for doc in docs:
            existing = session.run(
                "MATCH (c:Chunk {id:$id}) RETURN c.text_hash AS hash",
                id=doc["id"],
            ).single()
            if existing and existing.get("hash") == doc["hash"]:
                skipped += 1
                continue
            embedding = embedder.embed_query(doc["text"])
            session.run(
                """
                MERGE (c:Chunk {id:$id})
                SET c.text = $text,
                    c.source = $source,
                    c.dataset = $dataset,
                    c.embedding = $embedding,
                    c.text_hash = $hash
                """,
                id=doc["id"],
                text=doc["text"],
                source=doc["source"],
                dataset=DATASET_ID,
                embedding=embedding,
                hash=doc["hash"],
            )
            inserted += 1
    return inserted, skipped


def split_text_for_triples(text: str, max_length: int = 1024) -> List[str]:
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    if not paragraphs:
        paragraphs = [text]
    segments: List[str] = []
    for para in paragraphs:
        if len(para) <= max_length:
            segments.append(para)
            continue
        start = 0
        while start < len(para):
            end = min(len(para), start + max_length)
            segments.append(para[start:end])
            start = end
    return segments


def extract_triples(
    client: Client,
    text: str,
    model: str,
    language: str,
    retries: int = 2,
    allow_recursive: bool = True,
) -> List[Dict[str, str]]:
    prompt = TRIPLE_PROMPT_TEMPLATE.format(chunk=text, language=language)
    for attempt in range(retries + 1):
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.15 + attempt * 0.05, "top_p": 0.9},
        )
        content = response.get("message", {}).get("content", "")
        triples = parse_triples(content)
        if triples:
            return deduplicate_triples(triples)
    if allow_recursive and len(text) > 600:
        aggregated: List[Dict[str, str]] = []
        for segment in split_text_for_triples(text):
            partial = extract_triples(
                client,
                segment,
                model=model,
                language=language,
                retries=1,
                allow_recursive=False,
            )
            aggregated.extend(partial)
        return deduplicate_triples(aggregated)
    return []


def collect_triples_for_documents(
    client: Client, 
    docs: List[Dict[str, str]], 
    model: str, 
    language: str
) -> Tuple[Dict[str, List[Dict[str, str]]], List[str]]:
    """
    为所有文档批量提取三元组
    
    Args:
        client: Ollama client
        docs: 文档列表
        model: LLM 模型名称
        language: 目标语言
    
    Returns:
        (triple_map, empty_chunks): 三元组映射和无三元组的chunk列表
    """
    triple_map = {}
    empty_chunks = []
    
    for i, doc in enumerate(docs):
        print(f"   Extracting {i+1}/{len(docs)}...", end="\r")
        triples = extract_triples(client, doc["text"], model, language)
        
        if not triples:
            empty_chunks.append(doc["id"])
        
        triple_map[doc["id"]] = triples
    
    print(f"   ✅ 已处理 {len(docs)} 个文档，{len(empty_chunks)} 个无三元组")
    return triple_map, empty_chunks


def ingest_triples(
    driver,
    docs: List[Dict[str, str]],
    client: Client,
    model: str,
    language: str,
) -> Tuple[int, int, List[str]]:
    """
    增量式知識圖譜構建 (Incremental Construction)
    
    核心原則：
    1. 使用 MERGE 而非 CREATE，確保實體和關係不重複
    2. 不刪除既有的 MENTIONS 和 RELATION
    3. 僅增量添加新的知識三元組
    4. 保留所有歷史來源追溯 (r.chunks)
    
    階段劃分：
    - 階段一：實體節點增量寫入 (Entity Nodes)
    - 階段二：關係/三元組增量寫入 (Relationships/Triples)
    - 階段三：Chunk 與出處增量連接 (Provenance Linking)
    """
    triple_map, empty_chunks = collect_triples_for_documents(client, docs, model, language)
    updated = 0
    
    with driver.session() as session:
        for doc in docs:
            chunk_id = doc["id"]
            triples = triple_map.get(chunk_id, [])
            
            # ⚠️ 重要變更：移除所有 DELETE 操作
            # 舊邏輯（已廢除）：
            # - DELETE MENTIONS 關係
            # - DELETE RELATION 關係
            # 新邏輯：保留所有既有資料，僅增量添加
            
            if not triples:
                # 即使沒有新三元組，也不刪除既有資料
                continue
            
            # ═══════════════════════════════════════════════════════════
            # 階段一 + 階段二 + 階段三：合併執行（性能優化）
            # ═══════════════════════════════════════════════════════════
            session.run(
            """
            // ===== 階段一：實體節點增量寫入 =====
            UNWIND $triples AS triple
            
            // 創建或匹配頭實體（使用 MERGE 確保唯一性）
            MERGE (h:Entity {name: triple.head})
            ON CREATE SET h.created_at = timestamp()
            
            // 創建或匹配尾實體（使用 MERGE 確保唯一性）
            MERGE (t:Entity {name: triple.tail})
            ON CREATE SET t.created_at = timestamp()
            
            // ===== 階段二：關係/三元組增量寫入 =====
            // 使用 MERGE 確保關係唯一性（基於 head + type + tail）
            MERGE (h)-[r:RELATION {type: triple.relation}]->(t)
            ON CREATE SET 
                r.chunks = [$cid],
                r.created_at = timestamp(),
                r.confidence = 0.9
            ON MATCH SET 
                // 僅在 chunks 列表中不存在時才添加（避免重複）
                r.chunks = CASE 
                    WHEN $cid IN r.chunks THEN r.chunks 
                    ELSE r.chunks + $cid 
                END,
                r.last_updated = timestamp()
            
            // ===== 階段三：Chunk 與出處增量連接 =====
            WITH h, t
            
            // 確保 Chunk 節點存在
            MERGE (c:Chunk {id: $cid})
            
            // 增量連接 Chunk -> Entity (MENTIONS)
            // 使用 MERGE 確保關係不重複
            MERGE (c)-[:MENTIONS]->(h)
            MERGE (c)-[:MENTIONS]->(t)
            """,
            triples=triples,
            cid=chunk_id,
        )
            updated += 1
    
    skipped = len(docs) - updated
    return updated, skipped, empty_chunks


class GraphBuilder:
    """
    封装图谱构建流程
    """
    def __init__(self, driver, ollama_client: Client):
        self.driver = driver
        self.client = ollama_client
        self.embedder = OllamaVectorEmbedder(self.client, CONFIG["models"]["embed_model"])

    def build_graph(self, text_path: Path, chunk_size: int = None, overlap: int = None):
        """
        统一的图谱构建入口
        
        Args:
            text_path: 知识库文本路径
            chunk_size: chunk 大小（可选，默认使用 CONFIG）
            overlap: 重叠大小（可选，默认使用 CONFIG）
        """
        print("📚 Loading and chunking...")
        # ✅ 修正：將參數傳遞給 load_chunks
        chunks = load_chunks(text_path, chunk_size, overlap)
        print(f"  ✅ 已加载 {len(chunks)} 个 chunks")
        
        print("🧮 Ensuring indexes...")
        # ✅ 關鍵性能優化：為 Entity 創建索引
        ensure_entity_index(self.driver)
        
        ensure_vector_index(
            self.driver, 
            CONFIG["infrastructure"]["vector_index_name"], 
            "Chunk", 
            "embedding", 
            self.embedder.dimension
        )
        ensure_fulltext_index(
            self.driver,
            CONFIG["infrastructure"]["fulltext_index_name"],
            "Chunk",
            "text"
        )
        
        print("⬆️ Upserting chunks...")
        upserted, skipped = upsert_chunks(self.driver, self.embedder, chunks)
        print(f"  ✅ Upserted {upserted}, skipped {skipped}")
        
        print("🔗 Extracting triples...")
        updated, skipped_triples, empty = ingest_triples(
            self.driver, 
            chunks, 
            self.client, 
            CONFIG["models"]["llm_model"], 
            language=CONFIG["models"]["answer_language"]
        )
        print(f"  ✅ Updated {updated} chunks, {len(empty)} empty")
        
        print("\n✅ 图谱构建完成！")

