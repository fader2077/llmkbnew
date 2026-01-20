# src/retrieval.py
"""
Retrieval and QA evaluation (Phase 4)

核心改动：
- 移除 HybridRetriever 依赖
- 使用自定义 MultiHopRetriever
- 添加 0-hop 作为 Baseline (纯向量检索)
"""

import time
import pandas as pd
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

from neo4j_graphrag.retrievers.base import Retriever
from neo4j_graphrag.types import RawSearchResult, RetrieverResultItem

from config import CONFIG, RESULT_DIR
from src.models import OllamaVectorEmbedder


# ============================================================
# 1. 自定义多跳检索器 (MultiHopRetriever)
# ============================================================

class MultiHopRetriever(Retriever):
    """
    支持多跳推理的自定义检索器
    
    检索策略:
    - 0-hop: 纯向量检索 (Baseline)，只返回 Chunk
    - 1-hop: Chunk -> Entity (了解 Chunk 里有哪些实体)
    - 2-hop: Chunk -> Entity -> Neighbor Entity (Graph RAG 标准模式)
    - 3-hop: 深度遍历 (通常有噪声，适合消融对比)
    """
    
    def __init__(
        self,
        driver,
        vector_index_name: str,
        embedder,
        retrieval_depth: int = 0,  # 默认改为 0 (Baseline)
        max_entities_per_hop: int = 10,
        neo4j_database: str = None,
    ):
        self.driver = driver
        self.vector_index_name = vector_index_name
        self.embedder = embedder
        self.retrieval_depth = retrieval_depth
        self.max_entities_per_hop = max_entities_per_hop
        self.neo4j_database = neo4j_database
        
    def search(
        self,
        query_text: str = None,
        query_vector: List[float] = None,
        top_k: int = 5,
    ) -> RawSearchResult:
        """执行多跳检索"""
        
        # 1. 获取查询向量
        if query_vector is None and query_text is not None:
            query_vector = self.embedder.embed_query(query_text)
        
        # 2. 构建 Cypher 查询
        cypher_query = self._build_multihop_cypher()
        
        # 3. 执行查询
        with self.driver.session(database=self.neo4j_database) as session:
            result = session.run(
                cypher_query,
                vector_index_name=self.vector_index_name,
                query_vector=query_vector,
                top_k=top_k,
                max_entities=self.max_entities_per_hop
            )
            # 将 Neo4j Result 转为 list，符合 RawSearchResult 要求
            records = list(result)
        
        return RawSearchResult(records=records)
    
    def _build_multihop_cypher(self) -> str:
        """根据 retrieval_depth 构建不同的 Cypher 查询"""
        
        # ✅ 新增：0-hop (Vector Only Baseline)
        if self.retrieval_depth == 0:
            return """
            CALL db.index.vector.queryNodes($vector_index_name, $top_k, $query_vector)
            YIELD node, score
            // 0-hop 只返回 Chunk 节点本身
            RETURN node, score
            ORDER BY score DESC
            """
        
        elif self.retrieval_depth == 1:
            # 1-hop: Chunk -> Entity (检索 Chunk，并附带其包含的实体信息)
            return """
            CALL db.index.vector.queryNodes($vector_index_name, $top_k, $query_vector)
            YIELD node AS initial_chunk, score
            
            // 扩展到 MENTIONS 的实体
            OPTIONAL MATCH (initial_chunk)-[:MENTIONS]->(e:Entity)
            
            // 聚合返回
            WITH initial_chunk, score, collect(DISTINCT e.name) as entity_names
            
            // 返回标准格式
            RETURN initial_chunk as node, score, entity_names
            ORDER BY score DESC
            """
        
        elif self.retrieval_depth == 2:
            # 2-hop: Chunk -> Entity -> Neighbor Entity (Graph RAG 标准模式)
            return """
            CALL db.index.vector.queryNodes($vector_index_name, $top_k, $query_vector)
            YIELD node AS initial_chunk, score
            
            // 1. 找到该 Chunk 提到的实体
            MATCH (initial_chunk)-[:MENTIONS]->(e1:Entity)
            WITH initial_chunk, score, e1
            LIMIT $max_entities
            
            // 2. 扩展到邻居实体 (2-hop)
            OPTIONAL MATCH (e1)-[r:RELATION]->(e2:Entity)
            WITH initial_chunk, score, e1, e2
            LIMIT $max_entities * 2
            
            // 3. 找回包含这些邻居实体的 *其他* Chunks (扩充上下文)
            OPTIONAL MATCH (related_chunk:Chunk)-[:MENTIONS]->(e2)
            WHERE related_chunk <> initial_chunk
            
            // 聚合所有相关 Chunk
            WITH initial_chunk, score, collect(DISTINCT related_chunk) AS related_chunks
            
            // 展开并混合 (初始 Chunk + 关联 Chunk)
            UNWIND [initial_chunk] + related_chunks AS node
            
            // 对关联 Chunk 进行降权 (Decay)
            WITH node, 
                 CASE WHEN node = initial_chunk THEN score ELSE score * 0.7 END AS adjusted_score
            
            RETURN DISTINCT node, adjusted_score AS score
            ORDER BY score DESC
            LIMIT $top_k * 2
            """
        
        elif self.retrieval_depth == 3:
            # 3-hop: 深度遍历 (通常会有噪声，适合消融对比)
            return """
            CALL db.index.vector.queryNodes($vector_index_name, $top_k, $query_vector)
            YIELD node AS initial_chunk, score
            
            // 深度路径遍历 (1~2 层关系)
            MATCH path = (initial_chunk)-[:MENTIONS]->(e1:Entity)-[:RELATION*1..2]->(e_final:Entity)
            WITH initial_chunk, score, e_final, length(path) AS path_length
            LIMIT $max_entities * 3
            
            OPTIONAL MATCH (related_chunk:Chunk)-[:MENTIONS]->(e_final)
            WHERE related_chunk <> initial_chunk
            
            WITH initial_chunk, score, path_length, collect(DISTINCT related_chunk) AS related_chunks
            
            UNWIND [initial_chunk] + related_chunks AS node
            WITH node, 
                 CASE 
                     WHEN node = initial_chunk THEN score 
                     ELSE score * 0.5 
                 END AS adjusted_score
                 
            RETURN DISTINCT node, adjusted_score AS score
            ORDER BY score DESC
            LIMIT $top_k * 3
            """
        else:
            raise ValueError(f"不支持的 retrieval_depth: {self.retrieval_depth}")


# ============================================================
# 2. 辅助函数：上下文提取与处理
# ============================================================

def extract_contexts(raw_result: RawSearchResult, top_k: int) -> List[Dict[str, Any]]:
    """
    从 RawSearchResult 提取上下文
    
    Returns:
        List of dicts with keys: rank, score, text, chunk_id
    """
    contexts: List[Dict[str, Any]] = []
    
    if not raw_result or not raw_result.records:
        return contexts
    
    for rank, record in enumerate(raw_result.records[:top_k], start=1):
        node = record.get('node')
        score = record.get('score', 0.0)
        
        if node:
            # 从 Neo4j node 提取属性
            text = node.get('text', '')
            chunk_id = node.get('id', '')
            source = node.get('source', '')
            
            contexts.append({
                "rank": rank,
                "score": float(score) if score else 0.0,
                "text": text,
                "chunk_id": chunk_id,
                "source": source
            })
    
    return contexts


def expand_graph_context(driver, chunk_ids: List[str], limit_rel: int = 6) -> List[Dict[str, str]]:
    """
    扩展图谱上下文：为每个 Chunk 提取其 Entity 和 Relations
    
    用于增强可解释性
    """
    if not chunk_ids:
        return []
    
    with driver.session() as session:
        rows = session.run(
            """
            MATCH (c:Chunk)
            WHERE c.id IN $chunk_ids
            MATCH (c)-[:MENTIONS]->(e:Entity)
            OPTIONAL MATCH (e)-[r:RELATION]->(t:Entity)
            RETURN c.id AS chunk_id,
                   e.name AS entity,
                   collect({relation: r.type, tail: t.name})[0..$limit] AS relations
            """,
            chunk_ids=chunk_ids,
            limit=limit_rel,
        ).data()
    
    formatted: List[Dict[str, str]] = []
    for row in rows:
        relations = [
            f"{item.get('relation')}→{item.get('tail')}"
            for item in (row.get("relations") or [])
            if item.get("relation") and item.get("tail")
        ]
        formatted.append(
            {
                "chunk_id": row.get("chunk_id"),
                "entity": row.get("entity"),
                "relations": ", ".join(relations) if relations else "(无连结)",
            }
        )
    
    return formatted


# ============================================================
# 3. 高层检索引擎 (RetrievalEngine)
# ============================================================

@dataclass
class QAResult:
    """单次 QA 结果"""
    question: str
    predicted_answer: str
    reference_answer: Optional[str]
    hop: int
    top_k: int
    num_chunks: int
    inference_latency_ms: float
    contexts: List[Dict[str, Any]]


class RetrievalEngine:
    """
    负责单次问答与上下文生成
    """
    def __init__(self, driver, ollama_client):
        self.driver = driver
        self.ollama_client = ollama_client
        self.embedder = OllamaVectorEmbedder(
            ollama_client, 
            CONFIG["models"]["embed_model"]
        )
        self.llm_model = CONFIG["models"]["llm_model"]
        self.temperature = CONFIG["generation"]["temperature"]

    def run_qa(
        self, 
        question: str, 
        hop: int = 0, 
        top_k: int = 5,
        reference_answer: Optional[str] = None,
        verbose: bool = False
    ) -> QAResult:
        """
        执行单次 QA
        
        Args:
            question: 问题文本
            hop: 跳数 (0=baseline, 1=1-hop, 2=2-hop, 3=3-hop)
            top_k: 返回前 k 个 chunks
            reference_answer: 参考答案（用于评估）
            verbose: 是否打印详细信息
        
        Returns:
            QAResult
        """
        start_time = time.perf_counter()
        
        # 1. 初始化检索器
        retriever = MultiHopRetriever(
            driver=self.driver,
            vector_index_name=CONFIG["infrastructure"]["vector_index_name"],
            embedder=self.embedder,
            retrieval_depth=hop,
            max_entities_per_hop=CONFIG["retrieval"].get("max_nodes_per_hop", 10)
        )
        
        # 2. 检索
        raw_result = retriever.search(query_text=question, top_k=top_k)
        
        # 3. 提取上下文
        contexts = extract_contexts(raw_result, top_k)
        context_texts = [c["text"] for c in contexts if c["text"]]
        context_str = "\n\n".join(context_texts) if context_texts else "No context found."
        
        # 4. 生成回答
        answer = self._generate_answer(question, context_str)
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        result = QAResult(
            question=question,
            predicted_answer=answer,
            reference_answer=reference_answer,
            hop=hop,
            top_k=top_k,
            num_chunks=len(contexts),
            inference_latency_ms=elapsed_ms,
            contexts=contexts
        )
        
        if verbose:
            self._print_qa_result(result)
        
        return result

    def _generate_answer(self, question: str, context: str) -> str:
        """
        生成回答
        """
        system_instruction = (
            "Answer requirements:\n"
            f"1. Answer in {CONFIG['models']['answer_language']} naturally and fluently.\n"  # 自然流暢
            "2. Provide a concise but complete explanation based strictly on the context.\n" # 簡潔但完整
            "3. Include causality or reasoning if the question asks 'why' or 'how'.\n" # 包含因果推理
            "4. Do NOT use introductory phrases like 'Based on the text'.\n" # 去除廢話
        )
        
        prompt = f"""Context:
{context}

Question: {question}

{system_instruction}

Answer:"""
        
        try:
            response = self.ollama_client.chat(
                model=self.llm_model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": self.temperature, "top_p": 0.9},
            )
            content = response.get("message", {}).get("content", "")
            return content.strip()
        except Exception as e:
            return f"[Error: {e}]"
    
    def _print_qa_result(self, result: QAResult):
        """打印 QA 结果"""
        print(f"\n{'='*70}")
        print(f"❓ Question: {result.question}")
        print(f"🟩 Answer: {result.predicted_answer}")
        if result.reference_answer:
            print(f"📖 Reference: {result.reference_answer}")
        print(f"⚙️  Hop={result.hop}, Top-K={result.top_k}, Chunks={result.num_chunks}")
        print(f"⏱️  Latency: {result.inference_latency_ms:.1f} ms")
        
        if result.contexts:
            print(f"\n📄 Retrieved Chunks:")
            for ctx in result.contexts[:3]:  # 只显示前 3 个
                preview = (ctx.get("text") or "").replace("\n", " ")[:100]
                print(f"  #{ctx['rank']} [score={ctx['score']:.3f}] {preview}...")
        print("="*70)


# ============================================================
# 4. 简单的测试函数（用于手动测试）
# ============================================================

def test_retrieval(driver, ollama_client, question: str = "What are the symptoms of goat disease?"):
    """
    快速测试不同 hop 的检索效果
    """
    engine = RetrievalEngine(driver, ollama_client)
    
    print("\n🧪 Testing MultiHopRetriever with different hops...\n")
    
    for hop in [0, 1, 2, 3]:
        print(f"\n{'='*70}")
        print(f"🎯 Testing Hop-{hop} {'(Baseline - Vector Only)' if hop == 0 else ''}")
        print("="*70)
        
        result = engine.run_qa(
            question=question,
            hop=hop,
            top_k=5,
            verbose=True
        )
