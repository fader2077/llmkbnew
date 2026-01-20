# src/optimizer.py
"""
Structure augmentation (Phase 3)
圖譜優化器：負責增強連通性、實體對齊與圖譜清理
"""

import json
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from ollama import Client
from src.utils import parse_triples

# 設定 Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# Prompt 定義
# ==============================================================================

RELATION_ENHANCEMENT_PROMPT = """
You are an expert knowledge graph engineer.
Task: Extract **implicit relationships** between the provided entities based on the context.

⚠️ **CRITICAL CONSTRAINT**: You can ONLY use entity names from the following list to construct triples.

## 📋 Available Entity List
{entity_list}

## 📝 Context
{chunk_text}

## 📤 Output Format
Output ONLY a JSON array of triples:
[
  {{"head": "EntityA", "relation": "CAUSES", "tail": "EntityB"}}
]
"""

ENTITY_RESOLUTION_PROMPT = """
You are a data cleaning expert.
Task: Identify distinct entities that refer to the same concept (Synonyms) from the list below.
Focus on:
1. Plural forms (e.g., "Goat" and "Goats")
2. Abbreviations (e.g., "Vit A" and "Vitamin A")
3. Case sensitivity issues.

List: {entity_list}

Output ONLY a JSON list of pairs to merge. The 'primary' should be the most standard/complete name.
Example:
[
    {{"primary": "Vitamin A", "duplicate": "Vit A"}},
    {{"primary": "Goat", "duplicate": "goats"}}
]
If no duplicates found, return [].
"""

WEAK_ENTITY_INFERENCE_PROMPT = """
You are an expert knowledge graph reasoning agent.

Task: Infer potential relationships for a weakly-connected entity based on its existing connections and global context.

⚠️ **CRITICAL CONSTRAINT**: You can ONLY create relationships between entities from the PROVIDED ENTITY LIST.

## 📌 Target Weak Entity
Name: {weak_entity}
Current Connections: {current_connections}

## 📋 Available Entities for New Relations
{entity_list}

## 🧠 Reasoning Guidelines
1. **Based on current connections**: What other relationships can be inferred from the entity's existing neighbors?
2. **Domain knowledge**: Apply your domain expertise to suggest semantically valid relationships
3. **Transitive inference**: If A relates to B and B relates to C, might A relate to C?
4. **Avoid creating duplicate relationships** that already exist

## 📤 Output Format
Output ONLY a JSON array (max 5 relationships):
[
  {{"head": "weak_entity_name", "relation": "RELATIONSHIP_TYPE", "tail": "EntityFromList"}},
  {{"head": "EntityFromList", "relation": "RELATIONSHIP_TYPE", "tail": "weak_entity_name"}}
]

Focus on HIGH-CONFIDENCE inferences only. If uncertain, return fewer relationships or [].
"""

# 🚀 新增：優化過的批次處理 Prompt（以 Chunk 為中心）
WEAK_LINK_BATCH_PROMPT = """
You are a Knowledge Graph Expert.
Task: Connect the following "Isolated Entities" to the rest of the concepts in the text.

## 📄 Context Text:
{text}

## 🎯 Target Isolated Entities (Connect these!):
{entities}

## ⚡ Instructions:
1. For each Target Entity, find **explicit or implied** relationships connecting it to ANY other entity in the text.
2. The output must be valid JSON triples.
3. Use precise predicates (e.g., 'PART_OF', 'CAUSES', 'LOCATED_AT', 'HAS_SYMPTOM', 'TREATED_BY').
4. Focus on creating meaningful connections that integrate isolated entities into the knowledge graph.

## 📤 Output JSON format:
[
  {{"head": "IsolatedEntity", "relation": "RELATION", "tail": "OtherEntity"}},
  {{"head": "OtherEntity", "relation": "RELATION", "tail": "IsolatedEntity"}}
]

Extract as many valid relationships as possible to maximize connectivity.
"""

HYPOTHETICAL_QUESTIONS_PROMPT = """
You are an expert in knowledge graph relation extraction.

Task: Re-extract relationships between entities in this text using **hypothetical question-driven reasoning**.

## 🎯 Hypothetical Question Types
1. **Causal Questions**: Does X cause/lead to/result in Y? Does Y prevent/inhibit X?
2. **Compositional Questions**: Does X contain/include/consist of Y? Is Y a part/component of X?
3. **Functional Questions**: Does X use/require/depend on Y? Does X produce/generate Y?
4. **Hierarchical Questions**: Is X a type of Y? Does X belong to category Y?
5. **Comparative Questions**: How does X compare to Y? Is X similar to/different from Y?
6. **Temporal Questions**: Does X happen before/after/during Y?
7. **Spatial Questions**: Where is X located relative to Y?
8. **Attribute Questions**: What properties/characteristics does X have?

## 📋 Entities in this Chunk
{entity_list}

## 📝 Text Context
{chunk_text}

## 📤 Output Format
Output ONLY a JSON array of triples:
[
  {{"head": "EntityA", "relation": "CAUSES", "tail": "EntityB"}},
  {{"head": "EntityA", "relation": "CONTAINS", "tail": "EntityC"}}
]

Rules:
1. Only extract relationships explicitly supported by the text
2. Use specific relationship types (not generic like "related" or "associated")
3. Extract as many valid relationships as possible between entities
4. Focus on maximizing the number of valid connections to create a dense knowledge network
"""


# ==============================================================================
# 優化器類別
# ==============================================================================

class GraphOptimizer:
    """
    圖譜優化控制器
    包含：實體對齊、關係強化、孤立點清理
    
    🚀 優化版本特性：
    - 批次處理：以 Chunk 為單位批量處理弱實體
    - 並行執行：使用多線程加速 LLM 推理
    - 功能整合：同時完成弱連接修復和隱性關係挖掘
    """
    def __init__(self, driver, client: Client, model: str, max_workers: int = 2):
        self.driver = driver
        self.client = client
        self.model = model
        # 並行度設定（根據您的硬體調整）
        # GPU 本地運行建議 2-4，API 服務可設更高（如 8-10）
        self.max_workers = max_workers
        logging.info(f"GraphOptimizer initialized with {max_workers} workers")

    def run_optimization_pipeline(self, max_iterations: int = 1, dataset_id: str = "goat_kb_v1", use_accelerated: bool = True):
        """
        執行完整的 Phase 3 優化流程
        
        Args:
            max_iterations: 優化迭代次數
            dataset_id: 資料集ID
            use_accelerated: 是否使用加速版弱連接推理（預設True）
        """
        print(f"\n⚡ 開始 Phase 3 圖譜優化 (Max Iterations: {max_iterations})")
        print(f"   模式：{'🚀 加速版' if use_accelerated else '標準版'}")
        
        for i in range(max_iterations):
            print(f"\n🔄 Iteration {i+1}/{max_iterations}")
            
            # 1. 實體對齊 (先清理，再連接)
            self.merge_synonym_entities()
            
            # 2. 關係強化 (增加連接)
            self.enhance_connectivity(dataset_id)
            
            # 3. 🚀 弱連接推理（使用加速版或標準版）
            if use_accelerated:
                self.infer_weak_links_accelerated(degree_threshold=2)
            else:
                # 如果您保留了舊版方法，可以在這裡調用
                print("  ⚠️  標準版弱連接推理已被加速版取代")
            
            # 4. 清理孤立點 (打掃戰場)
            self.prune_isolated_nodes()
            
        print("\n✅ Phase 3 優化流程完成！")

    # --------------------------------------------------------------------------
    # 1. 實體對齊 (Entity Resolution)
    # --------------------------------------------------------------------------
    def merge_synonym_entities(self):
        """
        使用 LLM 識別相似實體並在 Neo4j 中合併
        """
        print("  🧩 執行實體對齊 (Entity Resolution)...")
        with self.driver.session() as session:
            # 抓取所有實體名稱
            entities = [r["name"] for r in session.run("MATCH (e:Entity) RETURN e.name AS name")]
        
        if not entities:
            print("    ⚠️ 無實體，跳過")
            return

        # 簡單分批處理
        batch_size = 200
        merged_count = 0
        
        for i in range(0, len(entities), batch_size):
            batch = entities[i : i + batch_size]
            prompt = ENTITY_RESOLUTION_PROMPT.format(entity_list=batch)
            
            try:
                response = self.client.chat(
                    model=self.model, 
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": 0.0}
                )
                content = response['message']['content'] if isinstance(response, dict) else ''
                
                # 解析 JSON
                pairs = []
                try:
                    json_str = content[content.find('['):content.rfind(']')+1]
                    pairs = json.loads(json_str)
                except Exception:
                    pairs = []
                
                if not pairs:
                    continue

                with self.driver.session() as session:
                    for p in pairs:
                        primary = p.get("primary")
                        duplicate = p.get("duplicate")
                        
                        if primary and duplicate and primary != duplicate:
                            # 檢查兩者是否都存在（優化：避免笛卡爾積）
                            check_result = session.run("""
                                MATCH (p:Entity {name: $primary})
                                MATCH (d:Entity {name: $duplicate})
                                RETURN count(p) + count(d) as cnt
                            """, primary=primary, duplicate=duplicate).single()
                            
                            if not check_result or check_result["cnt"] < 2:
                                continue

                            # 使用標準 Cypher 手動合併 (拆分為多步驟，避免 NULL 問題)
                            try:
                                # 步驟 1: 轉移出邊 (RELATION 關係)
                                session.run("""
                                    MATCH (p:Entity {name: $primary})
                                    MATCH (d:Entity {name: $duplicate})
                                    MATCH (d)-[r:RELATION]->(target)
                                    WITH p, d, r, target, type(r) as rel_type, properties(r) as props
                                    MERGE (p)-[new_r:RELATION {type: rel_type}]->(target)
                                    ON CREATE SET new_r = props
                                """, primary=primary, duplicate=duplicate)
                                
                                # 步驟 2: 轉移入邊 (RELATION 關係)
                                session.run("""
                                    MATCH (p:Entity {name: $primary})
                                    MATCH (d:Entity {name: $duplicate})
                                    MATCH (source)-[r:RELATION]->(d)
                                    WITH p, d, r, source, type(r) as rel_type, properties(r) as props
                                    MERGE (source)-[new_r:RELATION {type: rel_type}]->(p)
                                    ON CREATE SET new_r = props
                                """, primary=primary, duplicate=duplicate)
                                
                                # 步驟 3: 轉移 MENTIONS 關係
                                session.run("""
                                    MATCH (p:Entity {name: $primary})
                                    MATCH (d:Entity {name: $duplicate})
                                    MATCH (c:Chunk)-[m:MENTIONS]->(d)
                                    MERGE (c)-[:MENTIONS]->(p)
                                """, primary=primary, duplicate=duplicate)
                                
                                # 步驟 4: 刪除舊節點（會自動刪除所有關係）
                                session.run("""
                                    MATCH (d:Entity {name: $duplicate})
                                    DETACH DELETE d
                                """, duplicate=duplicate)
                                
                                merged_count += 1
                                print(f"    🔄 Merged: {duplicate} -> {primary}")
                            except Exception as e:
                                print(f"    ⚠️ Merge error: {e}")

            except Exception as e:
                print(f"    ⚠️ 批次處理錯誤: {e}")
                continue
        
        print(f"    ✅ 已合併 {merged_count} 組重複實體")

    # --------------------------------------------------------------------------
    # 2. 關係強化 (Connectivity Enhancement)
    # --------------------------------------------------------------------------
    def enhance_connectivity(self, dataset_id: str):
        """針對現有 Chunk 進行二次關係推理"""
        print("  🔗 執行關係強化 (Connectivity Enhancement)...")
        
        with self.driver.session() as session:
            # 獲取實體列表供 Prompt 使用
            entities_data = session.run("MATCH (e:Entity) RETURN e.name as name").data()
            entity_list = [e['name'] for e in entities_data]
            
            # 獲取 chunks
            chunks = session.run("""
                MATCH (c:Chunk {dataset: $dataset}) 
                RETURN c.id as id, c.text as text 
            """, dataset=dataset_id).data()

        if not chunks:
            print("    ⚠️ 無 Chunks，跳過")
            return

        added_count = 0
        formatted_entities = str(entity_list[:500])  # 截斷以防 Prompt 過長
        
        for chunk in chunks:
            prompt = RELATION_ENHANCEMENT_PROMPT.format(
                entity_list=formatted_entities,
                chunk_text=chunk['text']
            )
            
            try:
                response = self.client.chat(
                    model=self.model, 
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": 0.0}
                )
                content = response['message']['content'] if isinstance(response, dict) else ''
                triples = parse_triples(content)
                
                with self.driver.session() as session:
                    for t in triples:
                        # 只連接現有實體（修復：分開 MATCH 避免笛卡爾積）
                        result = session.run("""
                            MATCH (h:Entity {name: $head})
                            MATCH (t:Entity {name: $tail})
                            MERGE (h)-[r:RELATION {type: $rel}]->(t)
                            ON CREATE SET r.enhanced = true, r.confidence = 0.8
                            RETURN r
                        """, head=t['head'], rel=t['relation'], tail=t['tail'])
                        
                        if result.single():
                            added_count += 1
            except Exception:
                continue
                
        print(f"    ✅ 推理並新增了 {added_count} 條關係")

    # --------------------------------------------------------------------------
    # 3. 孤立點清理 (Pruning)
    # --------------------------------------------------------------------------
    def prune_isolated_nodes(self):
        """刪除沒有任何關係的孤立 Entity 節點"""
        print("  ✂️  執行孤立點清理 (Pruning)...")
        with self.driver.session() as session:
            # 刪除沒有 RELATION 且沒有 MENTIONS 的實體 (完全孤立)
            result = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)--()
                DELETE e
                RETURN count(e) as cnt
            """)
            record = result.single()
            cnt = record["cnt"] if record else 0
            
            print(f"    ✅ 已刪除 {cnt} 個完全孤立實體")

    # --------------------------------------------------------------------------
    # 🚀 新增：加速版弱連接推理 (Context-Aware Batching + Parallel Execution)
    # --------------------------------------------------------------------------
    def infer_weak_links_accelerated(self, degree_threshold: int = 2):
        """
        🚀 加速版：弱連接推理 (整合了上下文批次處理與並行執行)
        
        核心優化：
        1. 批次處理：以 Chunk 為單位，一次處理多個弱實體
        2. 並行執行：使用 ThreadPoolExecutor 同時處理多個 Chunks
        3. 功能整合：同時完成弱連接修復和隱性關係挖掘
        
        Args:
            degree_threshold: 連接數閾值，低於此值視為弱實體（預設 2）
        """
        print(f"\n{'='*60}")
        print(f"🚀 啟動加速版圖譜擴增 (Target: Weak Entities < {degree_threshold} links)")
        print(f"   策略：Context-Aware Batching + Parallel Execution")
        print(f"   並行度：{self.max_workers} workers")
        print(f"{'='*60}")

        # 1. 抓取資料：找出「包含弱實體」的 Chunks，並將弱實體按 Chunk 分組
        # 這句 Cypher 非常關鍵，它直接把工作量按 Chunk 分好了
        fetch_query = """
        MATCH (e:Entity)
        WHERE size((e)--()) < $threshold
        MATCH (e)<-[:MENTIONS]-(c:Chunk)
        WITH c, collect(DISTINCT e.name) AS weak_entities
        WHERE size(weak_entities) > 0
        RETURN c.id AS chunk_id, c.text AS text, weak_entities
        """
        
        with self.driver.session() as session:
            result = session.run(fetch_query, threshold=degree_threshold)
            tasks = [record.data() for record in result]

        if not tasks:
            print("📊 未發現需要處理的弱實體，跳過優化")
            return

        print(f"📊 掃描完成：共 {len(tasks)} 個 Chunks 包含弱連接實體，準備並行處理...")
        logging.info(f"Found {len(tasks)} chunks with weak entities")

        total_new_relations = 0
        
        # 2. 定義單個任務的處理函數 (給執行緒用)
        def process_chunk_task(task):
            chunk_id = task['chunk_id']
            text = task['text']
            weak_entities = task['weak_entities']
            
            # 如果該 Chunk 的弱實體太多，可以截斷以免 Prompt 太長
            # 建議最多處理 20 個實體/Chunk
            if len(weak_entities) > 20:
                weak_entities = weak_entities[:20]
                logging.warning(f"Chunk {chunk_id} has too many weak entities, truncated to 20")
            
            target_list_str = ", ".join(weak_entities)
            
            prompt = WEAK_LINK_BATCH_PROMPT.format(text=text, entities=target_list_str)
            
            try:
                response = self.client.chat(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": 0.1}  # 低隨機性，求穩
                )
                content = response['message']['content'] if isinstance(response, dict) else ''
                triples = parse_triples(content)
                logging.debug(f"Chunk {chunk_id}: extracted {len(triples)} triples")
                return triples
            except Exception as e:
                # 靜默失敗或記錄 Log，不要卡住主流程
                logging.error(f"Error processing chunk {chunk_id}: {e}")
                return []

        # 3. 使用 ThreadPoolExecutor 並行執行
        new_triples_batch = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任務
            future_to_chunk = {executor.submit(process_chunk_task, task): task for task in tasks}
            
            # 使用 tqdm 顯示進度條
            for future in tqdm(as_completed(future_to_chunk), total=len(tasks), desc="🔄 Processing Chunks"):
                triples = future.result()
                if triples:
                    new_triples_batch.extend(triples)

        # 4. 批次寫入資料庫 (減少 DB I/O)
        if new_triples_batch:
            print(f"\n💾 正在將 {len(new_triples_batch)} 條新關係寫入 Neo4j...")
            total_new_relations = self._batch_insert_relations(new_triples_batch)
            print(f"   ✅ 成功寫入 {total_new_relations} 條關係")
        else:
            print("\n⚠️  未產生新關係")

        print(f"\n✅ 優化完成！新增了 {total_new_relations} 條關係，強化了弱實體連接。")
        logging.info(f"Weak link inference completed: {total_new_relations} new relations added")

    def _batch_insert_relations(self, triples: List[Dict], batch_size: int = 1000) -> int:
        """
        輔助函數：分批寫入關係，避免記憶體溢出
        
        Args:
            triples: 三元組列表
            batch_size: 每批寫入的數量
            
        Returns:
            成功寫入的關係數量
        """
        inserted_count = 0
        
        with self.driver.session() as session:
            for i in range(0, len(triples), batch_size):
                batch = triples[i:i+batch_size]
                
                for item in batch:
                    try:
                        # 清理關係名稱（轉為大寫並替換空格）
                        rel_type = item.get('relation', 'RELATED_TO').upper().replace(" ", "_").replace("-", "_")
                        if not rel_type or rel_type == "":
                            rel_type = "RELATED_TO"
                        
                        # 動態創建關係（使用 Cypher 字串插值，需謹慎處理）
                        # 注意：這裡使用參數化查詢保證安全性
                        cypher = f"""
                        MATCH (h:Entity {{name: $head}})
                        MATCH (t:Entity {{name: $tail}})
                        WHERE h <> t
                        MERGE (h)-[r:`{rel_type}`]->(t)
                        ON CREATE SET r.source = 'ai_inference', r.confidence = 0.8
                        RETURN r
                        """
                        result = session.run(cypher, head=item['head'], tail=item['tail'])
                        
                        if result.single():
                            inserted_count += 1
                            
                    except Exception as e:
                        # 跳過失敗的關係，繼續處理下一個
                        logging.debug(f"Failed to insert relation {item}: {e}")
                        continue
        
        return inserted_count

    # --------------------------------------------------------------------------
    # 6. 質量問題修復 (Quality Issue Fixes)
    # --------------------------------------------------------------------------
    def fix_quality_issues(self) -> Dict[str, int]:
        """
        修復圖譜中的質量問題
        
        修復項目：
        1. 自環關係（實體指向自己）
        2. 重複關係（相同 head + type + tail）
        3. 缺少來源標記的關係（r.chunks 為空）
        
        Returns:
            包含修復統計的字典
        """
        print("\n🔧 開始質量問題修復...")
        print("="*70)
        
        results = {
            'self_loops_removed': 0,
            'duplicate_relations_merged': 0,
            'empty_chunks_fixed': 0
        }
        
        with self.driver.session() as session:
            # ═══════════════════════════════════════════════════════════════
            # 修復 1：移除自環關係
            # ═══════════════════════════════════════════════════════════════
            print("\n🔍 修復 1：移除自環關係")
            print("-"*70)
            
            self_loops_count = session.run("""
                MATCH (e:Entity)-[r:RELATION]->(e)
                RETURN count(r) AS cnt
            """).single()["cnt"]
            
            if self_loops_count > 0:
                print(f"  發現 {self_loops_count} 個自環關係，正在移除...")
                result = session.run("""
                    MATCH (e:Entity)-[r:RELATION]->(e)
                    DELETE r
                    RETURN count(r) AS deleted
                """)
                record = result.single()
                deleted = record["deleted"] if record else 0
                results['self_loops_removed'] = deleted
                print(f"  ✅ 已移除 {deleted} 個自環關係")
            else:
                print("  ✅ 未發現自環關係")
            
            # ═══════════════════════════════════════════════════════════════
            # 修復 2：合併重複關係
            # ═══════════════════════════════════════════════════════════════
            print(f"\n🔍 修復 2：合併重複關係")
            print("-"*70)
            
            # 找出重複關係組
            duplicate_groups = session.run("""
                MATCH (h:Entity)-[r:RELATION]->(t:Entity)
                WITH h, t, r.type AS rel_type, collect(r) AS rels
                WHERE size(rels) > 1
                RETURN h.name AS head, t.name AS tail, rel_type, size(rels) AS dup_count
            """).data()
            
            if duplicate_groups:
                print(f"  發現 {len(duplicate_groups)} 組重複關係，正在合併...")
                
                for group in duplicate_groups:
                    head = group['head']
                    tail = group['tail']
                    rel_type = group['rel_type']
                    
                    # 合併策略：保留第一個關係，合併 chunks 屬性，刪除其餘關係
                    session.run("""
                        MATCH (h:Entity {name: $head})-[r:RELATION {type: $rel_type}]->(t:Entity {name: $tail})
                        WITH h, t, $rel_type AS rel_type, collect(r) AS rels
                        WHERE size(rels) > 1
                        
                        // 收集所有 chunks
                        WITH h, t, rels, 
                             [rel IN rels | COALESCE(rel.chunks, [])] AS all_chunks_list
                        WITH h, t, rels, 
                             reduce(acc = [], chunks IN all_chunks_list | acc + chunks) AS merged_chunks
                        
                        // 保留第一個關係，更新其 chunks
                        WITH rels[0] AS keep_rel, rels[1..] AS delete_rels, merged_chunks
                        SET keep_rel.chunks = merged_chunks
                        
                        // 刪除其餘關係
                        FOREACH (r IN delete_rels | DELETE r)
                        
                        RETURN size(delete_rels) AS deleted
                    """, head=head, tail=tail, rel_type=rel_type)
                
                results['duplicate_relations_merged'] = len(duplicate_groups)
                print(f"  ✅ 已合併 {len(duplicate_groups)} 組重複關係")
            else:
                print("  ✅ 未發現重複關係")
            
            # ═══════════════════════════════════════════════════════════════
            # 修復 3：修復缺少來源標記的關係（強力雙策略模式）
            # ═══════════════════════════════════════════════════════════════
            print("\n🔍 修復 3：修復缺少來源標記的關係（強力模式）")
            print("-" * 70)
            
            # 先檢查有多少需要修復
            total_empty = session.run("""
                MATCH ()-[r:RELATION]->() 
                WHERE r.chunks IS NULL OR size(r.chunks) = 0 
                RETURN count(r) as cnt
            """).single()["cnt"]
            print(f"  發現 {total_empty:,} 個關係缺少來源標記")
            
            if total_empty > 0:
                # 策略 A：優先找「共同」Chunks（精準模式）
                print(f"\n  🔹 策略 A：查找頭尾實體共同出現的 Chunks（精準模式）...")
                strategy_a = session.run("""
                    MATCH (h:Entity)-[r:RELATION]->(t:Entity)
                    WHERE r.chunks IS NULL OR size(r.chunks) = 0
                    MATCH (c:Chunk)-[:MENTIONS]->(h)
                    MATCH (c)-[:MENTIONS]->(t)
                    WITH r, collect(DISTINCT c.id) AS common_chunks
                    WHERE size(common_chunks) > 0
                    SET r.chunks = common_chunks
                    RETURN count(r) AS cnt
                """).single()["cnt"]
                print(f"     ✅ 策略 A 修復了: {strategy_a:,} 個")
                
                # 策略 B：繼承頭尾實體的所有來源（寬鬆模式，確保不斷鏈）
                print(f"\n  🔹 策略 B：繼承頭尾實體的所有來源（寬鬆模式）...")
                strategy_b = session.run("""
                    MATCH (h:Entity)-[r:RELATION]->(t:Entity)
                    WHERE r.chunks IS NULL OR size(r.chunks) = 0
                    
                    // 收集頭實體的來源
                    OPTIONAL MATCH (c1:Chunk)-[:MENTIONS]->(h)
                    WITH r, t, collect(DISTINCT c1.id) AS h_chunks
                    
                    // 收集尾實體的來源
                    OPTIONAL MATCH (c2:Chunk)-[:MENTIONS]->(t)
                    WITH r, h_chunks, collect(DISTINCT c2.id) AS t_chunks
                    
                    // 合併兩者並去重
                    WITH r, h_chunks + t_chunks AS all_chunks
                    WHERE size(all_chunks) > 0
                    
                    // 手動去重（不依賴 APOC）
                    WITH r, [x IN all_chunks WHERE x IS NOT NULL] AS filtered_chunks
                    WITH r, reduce(s = [], x IN filtered_chunks | 
                        CASE WHEN x IN s THEN s ELSE s + [x] END
                    ) AS unique_chunks
                    
                    SET r.chunks = unique_chunks
                    RETURN count(r) AS cnt
                """).single()["cnt"]
                print(f"     ✅ 策略 B 修復了: {strategy_b:,} 個")
                
                results['empty_chunks_fixed'] = strategy_a + strategy_b
                
                # 再次檢查是否還有無法修復的
                remaining = session.run("""
                    MATCH ()-[r:RELATION]->() 
                    WHERE r.chunks IS NULL OR size(r.chunks) = 0 
                    RETURN count(r) AS cnt
                """).single()["cnt"]
                
                print(f"\n  📊 修復統計：")
                print(f"     • 修復前：{total_empty:,} 個")
                print(f"     • 修復後：{remaining:,} 個")
                print(f"     • 成功修復：{total_empty - remaining:,} 個 ({(total_empty - remaining) / total_empty * 100:.1f}%)")
                
                if remaining > 0:
                    print(f"\n  ⚠️  仍有 {remaining:,} 個關係無法修復")
                    print(f"     （可能是推理關係，且頭尾實體都是孤兒實體）")
                else:
                    print(f"\n  ✅ 所有關係都已成功補充來源標記！")
            else:
                print("  ✅ 所有關係都有來源標記，無需修復")
        
        print(f"\n{'='*70}")
        print(f"✅ 質量問題修復完成")
        print(f"  • 移除自環關係：{results['self_loops_removed']}")
        print(f"  • 合併重複關係：{results['duplicate_relations_merged']}")
        print(f"  • 修復來源標記：{results['empty_chunks_fixed']}")
        print(f"{'='*70}")
        
        return results

    # --------------------------------------------------------------------------
    # 4. 弱連接實體全局關係推理 (Weak Entity Augmentation)
    # --------------------------------------------------------------------------
    def infer_global_relations(
        self, 
        min_degree: int = 1, 
        max_degree: int = 3, 
        max_inferences_per_entity: int = 5,
        batch_size: int = 10
    ) -> Dict[str, Any]:
        """
        針對弱連接實體（度數 1-3）進行全局關係推理
        
        核心原則：
        1. 只針對現有的弱連接實體進行擴增
        2. 只在現有實體之間建立新關係（MATCH + MERGE，不 CREATE）
        3. 基於實體的鄰居上下文進行推理
        
        Args:
            min_degree: 最小度數（包含）
            max_degree: 最大度數（包含）
            max_inferences_per_entity: 每個實體最多推理幾條關係
            batch_size: 批次處理大小
            
        Returns:
            包含統計信息的字典
        """
        print(f"\n🧠 開始弱連接實體全局關係推理")
        print(f"  目標：針對弱連接實體（度數 {min_degree}-{max_degree}）推理新關係")
        print("="*70)
        
        # ═══════════════════════════════════════════════════════════════
        # 階段 1：識別弱連接實體
        # ═══════════════════════════════════════════════════════════════
        print(f"\n📊 階段 1：識別弱連接實體...")
        
        with self.driver.session() as session:
            # 統計強化前狀態
            stats_before = session.run("""
                MATCH (e:Entity)
                WITH count(e) AS total_entities
                MATCH ()-[r:RELATION]->()
                RETURN total_entities, count(r) AS total_relations,
                       toFloat(count(r)) / total_entities AS density
            """).single()
            
            density_before = stats_before['density'] if stats_before else 0.0
            total_entities = stats_before['total_entities'] if stats_before else 0
            
            # 識別弱連接實體
            weak_entities = session.run(f"""
                MATCH (e:Entity)
                OPTIONAL MATCH (e)-[r:RELATION]-()
                WITH e, count(DISTINCT r) AS degree
                WHERE degree >= {min_degree} AND degree <= {max_degree}
                RETURN e.name AS entity_name, degree
                ORDER BY degree ASC
            """).data()
            
            print(f"  找到 {len(weak_entities)} 個弱連接實體（度數 {min_degree}-{max_degree}）")
            
            if not weak_entities:
                print("  ⚠️  無符合條件的弱連接實體")
                return {
                    'processed_entities': 0,
                    'inferred_relations': 0,
                    'density_before': density_before,
                    'density_after': density_before
                }
            
            # 獲取所有實體列表（用於 Prompt）
            all_entities = [e["entity_name"] for e in session.run(
                "MATCH (e:Entity) RETURN e.name AS entity_name"
            ).data()]
            
        # ═══════════════════════════════════════════════════════════════
        # 階段 2：對弱連接實體進行關係推理
        # ═══════════════════════════════════════════════════════════════
        print(f"\n🔄 階段 2：對弱連接實體進行關係推理...")
        
        processed_count = 0
        total_inferred = 0
        
        for idx, entity_data in enumerate(weak_entities):
            entity_name = entity_data['entity_name']
            current_degree = entity_data['degree']
            
            # 獲取該實體的現有連接
            with self.driver.session() as session:
                current_connections = session.run("""
                    MATCH (e:Entity {name: $name})-[r:RELATION]-(neighbor:Entity)
                    RETURN type(r) AS rel_type, neighbor.name AS neighbor_name
                    LIMIT 20
                """, name=entity_name).data()
            
            # 格式化當前連接信息
            connections_str = "\n".join([
                f"  - {conn['rel_type']} -> {conn['neighbor_name']}" 
                for conn in current_connections
            ]) if current_connections else "  (No current connections)"
            
            # 格式化實體列表（限制長度）
            entity_list_str = ", ".join(all_entities[:300])
            
            # 構建提示詞
            prompt = WEAK_ENTITY_INFERENCE_PROMPT.format(
                weak_entity=entity_name,
                current_connections=connections_str,
                entity_list=entity_list_str
            )
            
            # 調用 LLM 推理
            try:
                response = self.client.chat(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": 0.2, "top_p": 0.9}  # 稍高溫度允許推理
                )
                content = response.get("message", {}).get("content", "") if isinstance(response, dict) else ""
                
                # 解析三元組
                inferred_triples = parse_triples(content)
                
                # 限制推理數量
                inferred_triples = inferred_triples[:max_inferences_per_entity]
                
                # 寫入新關係（只連接現有實體）
                with self.driver.session() as session:
                    for triple in inferred_triples:
                        head = triple.get("head")
                        relation = triple.get("relation")
                        tail = triple.get("tail")
                        
                        if not all([head, relation, tail]):
                            continue
                        
                        # 使用 MATCH + MERGE 確保不創建新實體
                        result = session.run("""
                            MATCH (h:Entity {name: $head})
                            MATCH (t:Entity {name: $tail})
                            MERGE (h)-[r:RELATION {type: $relation}]->(t)
                            ON CREATE SET r.inferred = true, r.confidence = 0.75
                            RETURN count(r) AS created
                        """, head=head, relation=relation, tail=tail)
                        
                        record = result.single()
                        if record and record['created'] > 0:
                            total_inferred += 1
                
                processed_count += 1
                
                if (idx + 1) % batch_size == 0:
                    print(f"  ↳ 已處理 {processed_count}/{len(weak_entities)} 個實體，推理 {total_inferred} 條關係")
                    
            except Exception as e:
                print(f"  ⚠️  Entity {entity_name} 推理失敗：{e}")
                continue
        
        # ═══════════════════════════════════════════════════════════════
        # 階段 3：統計結果
        # ═══════════════════════════════════════════════════════════════
        with self.driver.session() as session:
            stats_after = session.run("""
                MATCH (e:Entity)
                WITH count(e) AS total_entities
                MATCH ()-[r:RELATION]->()
                RETURN total_entities, count(r) AS total_relations,
                       toFloat(count(r)) / total_entities AS density
            """).single()
            
            density_after = stats_after['density'] if stats_after else 0.0
        
        print(f"\n{'='*70}")
        print(f"✅ 弱連接實體全局關係推理完成")
        print(f"  • 處理實體數：{processed_count}")
        print(f"  • 推理關係數：{total_inferred}")
        print(f"  • 密度變化：{density_before:.3f} → {density_after:.3f} (+{density_after-density_before:.3f})")
        print(f"{'='*70}")
        
        return {
            'processed_entities': processed_count,
            'inferred_relations': total_inferred,
            'density_before': density_before,
            'density_after': density_after
        }

    # --------------------------------------------------------------------------
    # 5. 假設性問題關係密集化 (Densification)
    # --------------------------------------------------------------------------
    def densify_relations_with_questions(
        self,
        dataset_id: str,
        target_chunks: int = 10000,
        temperature: float = 0.0
    ) -> Dict[str, Any]:
        """
        使用假設性問題法對低密度 Chunks 重新抽取關係
        
        策略：
        1. 識別關係密度較低的 Chunks（實體間連接不足）
        2. 使用假設性問題引導 LLM 發現更多隱含關係
        3. 只連接已存在的實體（MATCH + MERGE）
        
        Args:
            dataset_id: 數據集 ID
            target_chunks: 要處理的 Chunk 數量
            temperature: LLM 溫度
            
        Returns:
            包含統計信息的字典
        """
        print(f"\n💡 開始假設性問題關係密集化")
        print(f"  目標 Chunks 數：{target_chunks}")
        print(f"  抽取模型：{self.model}")
        print("="*70)
        
        # ═══════════════════════════════════════════════════════════════
        # 階段 1：選擇低密度 Chunks
        # ═══════════════════════════════════════════════════════════════
        print(f"\n📊 階段 1：選擇低密度 Chunks...")
        
        with self.driver.session() as session:
            # 記錄初始狀態
            initial_stats = session.run("""
                MATCH (e:Entity)
                WITH count(e) AS entities
                MATCH ()-[r:RELATION]->()
                RETURN entities, count(r) AS relations,
                       toFloat(count(r)) / entities AS density
            """).single()
            
            density_before = initial_stats['density'] if initial_stats else 0.0
            
            # 選擇低密度 Chunks（實體數 >= 3，但連接度 < 30%）
            low_density_chunks = session.run(f"""
                MATCH (c:Chunk {{dataset: $dataset}})-[:MENTIONS]->(e:Entity)
                WITH c, collect(DISTINCT e.name) AS entities, count(DISTINCT e) AS entity_count
                WHERE entity_count >= 3
                
                // 計算該 Chunk 中實體間的關係數
                MATCH (c)-[:MENTIONS]->(e1:Entity)
                MATCH (c)-[:MENTIONS]->(e2:Entity)
                WHERE e1 <> e2
                OPTIONAL MATCH (e1)-[r:RELATION]-(e2)
                WITH c, entities, entity_count,
                     count(DISTINCT r) AS relation_count,
                     toFloat(count(DISTINCT r)) / (entity_count * (entity_count - 1) / 2) AS chunk_density
                
                WHERE chunk_density < 0.3
                
                RETURN c.id AS chunk_id, c.text AS chunk_text,
                       entities, entity_count, relation_count, chunk_density
                ORDER BY entity_count DESC, chunk_density ASC
                LIMIT {target_chunks}
            """, dataset=dataset_id).data()
            
            print(f"  找到 {len(low_density_chunks)} 個低密度 Chunks")
            
            if not low_density_chunks:
                print("  ✅ 所有 Chunks 密度已達標")
                return {
                    'processed_chunks': 0,
                    'new_relations': 0,
                    'density_before': density_before,
                    'density_after': density_before
                }
        
        # ═══════════════════════════════════════════════════════════════
        # 階段 2：對每個 Chunk 進行密集化抽取
        # ═══════════════════════════════════════════════════════════════
        print(f"\n🔄 階段 2：使用假設性問題法重新抽取關係...")
        
        total_new_relations = 0
        processed_count = 0
        
        for idx, chunk_data in enumerate(low_density_chunks):
            chunk_id = chunk_data['chunk_id']
            chunk_text = chunk_data['chunk_text']
            entities = chunk_data['entities']
            
            # 格式化實體列表
            entity_list_text = ", ".join(entities)
            
            # 構建提示詞
            prompt = HYPOTHETICAL_QUESTIONS_PROMPT.format(
                chunk_text=chunk_text[:2000],  # 限制長度
                entity_list=entity_list_text
            )
            
            # 調用 LLM
            try:
                response = self.client.chat(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": temperature, "top_p": 0.9}
                )
                content = response.get("message", {}).get("content", "") if isinstance(response, dict) else ""
                
                # 解析三元組
                triples = parse_triples(content)
                
                # 寫入新關係
                with self.driver.session() as session:
                    for triple in triples:
                        head = triple.get("head")
                        relation = triple.get("relation")
                        tail = triple.get("tail")
                        
                        if not all([head, relation, tail]):
                            continue
                        
                        # 只連接現有實體
                        result = session.run("""
                            MATCH (h:Entity {name: $head})
                            MATCH (t:Entity {name: $tail})
                            MERGE (h)-[r:RELATION {type: $relation}]->(t)
                            ON CREATE SET r.chunks = [$chunk_id], r.densified = true
                            ON MATCH SET r.chunks = CASE
                                WHEN NOT $chunk_id IN r.chunks
                                THEN r.chunks + [$chunk_id]
                                ELSE r.chunks
                            END
                            RETURN count(r) AS created
                        """, head=head, relation=relation, tail=tail, chunk_id=chunk_id)
                        
                        record = result.single()
                        if record:
                            total_new_relations += record['created']
                
                processed_count += 1
                
                if (idx + 1) % 10 == 0:
                    print(f"  ↳ 已處理 {processed_count}/{len(low_density_chunks)} Chunks，新增 {total_new_relations} 個關係")
                    
            except Exception as e:
                print(f"  ⚠️  Chunk {chunk_id} 處理失敗：{e}")
                continue
        
        # ═══════════════════════════════════════════════════════════════
        # 階段 3：統計結果
        # ═══════════════════════════════════════════════════════════════
        with self.driver.session() as session:
            final_stats = session.run("""
                MATCH (e:Entity)
                WITH count(e) AS entities
                MATCH ()-[r:RELATION]->()
                RETURN entities, count(r) AS relations,
                       toFloat(count(r)) / entities AS density
            """).single()
            
            density_after = final_stats['density'] if final_stats else 0.0
        
        print(f"\n{'='*70}")
        print(f"✅ 假設性問題關係密集化完成")
        print(f"  • 處理 Chunks：{processed_count}")
        print(f"  • 新增關係：{total_new_relations}")
        print(f"  • 密度變化：{density_before:.3f} → {density_after:.3f} (+{density_after-density_before:.3f})")
        print(f"{'='*70}")
        
        return {
            'processed_chunks': processed_count,
            'new_relations': total_new_relations,
            'density_before': density_before,
            'density_after': density_after
        }

