# Structure augmentation (Phase 3) (placeholder)

# ═══════════════════════════════════════════════════════════════════
# Relation Enhancement Prompt: Focus on Existing Entity Relations
# ═══════════════════════════════════════════════════════════════════

RELATION_ENHANCEMENT_PROMPT = """
You are an expert in knowledge graph relation extraction. Your task is to extract **relationships between entities** from the text, but with a strict constraint:

⚠️ **CRITICAL CONSTRAINT**: You can ONLY use entity names from the following list to construct triples.

## 📋 Available Entity List (MUST STRICTLY FOLLOW)

{entity_list}

## 🎯 Extraction Rules

1. **Entity Matching**:
   - The 'head' and 'tail' of each triple MUST **exactly match** an entity name from the above list
   - If the text mentions a concept NOT in the list, **DO NOT extract** that triple
   - Perform synonym matching (e.g., "goat" = "goats" = "caprine")

2. **Relation Type Normalization**:
   - Use specific, precise verbs (e.g., "causes", "contains", "requires", "belongs_to")
   - Avoid vague verbs (e.g., "relates_to", "associated_with", "affects")

3. **Deep Mining**:
   - **Explicit relations**: Directly extracted from text statements
   - **Implicit relations**: Causal, classification, and compositional relations based on logical reasoning
   - **Attribute relations**: Numerical, state, and feature-based descriptive relations

4. **Quality First**:
   - Each triple must be semantically clear and logically rigorous
   - Prioritize relations between core concepts
   - Avoid overly granular relations (e.g., "goat"-"weight"-"45" can be simplified to "goat"-"weighs"-"45kg")

## 📤 Output Format

Output ONLY a JSON array, with each triple containing head, relation, and tail fields:

```json
[
  {{"head":"goat", "relation":"deficient_in", "tail":"vitamin_A"}},
  {{"head":"vitamin_A_deficiency", "relation":"causes", "tail":"growth_retardation"}},
  {{"head":"goat", "relation":"belongs_to", "tail":"ruminant"}}
]
```

## 📝 Text to Extract From

{chunk_text}

Begin extraction. Remember: **ONLY use entity names from the available entity list**!
"""


def format_entity_list_for_prompt(entities: List[str], max_entities: int = 10000) -> str:
    """
    Format entity list for prompt readability
    
    Args:
        entities: List of entity names
        max_entities: Maximum number of entities to display (avoid excessively long prompts)
    
    Returns:
        Formatted entity list string
    """
    if len(entities) <= max_entities:
        entity_str = "\n".join([f"  • {entity}" for entity in entities])
        return f"(Total: {len(entities)} entities)\n\n{entity_str}"
    else:
        # If too many entities, show first N + total count
        sample_entities = entities[:max_entities]
        entity_str = "\n".join([f"  • {entity}" for entity in sample_entities])
        return f"(Total: {len(entities)} entities, showing first {max_entities})\n\n{entity_str}\n\n... and {len(entities) - max_entities} more entities"


print("✅ Relation enhancement prompt loaded")
# ═══════════════════════════════════════════════════════════════════
# 核心函數：EnhanceGraphConnectivity()
# ═══════════════════════════════════════════════════════════════════

def EnhanceGraphConnectivity(
    driver,
    client: Client,
    model: str,
    dataset_id: str = "goat_kb_v1",
    max_entities_per_prompt: int = 200,
    temperature: float = 0.2,
    batch_size: int = 5,
) -> Dict[str, Any]:
    """
    關係強化主函數：基於現有 Chunk 和 Entity 進行二次關係抽取
    
    核心原則：
    1. 只連接、不創建（No CREATE, Only MATCH + MERGE）
    2. 只對現有實體建立關係
    3. 增量寫入，避免重複
    
    Args:
        driver: Neo4j driver
        client: Ollama client
        model: LLM model name
        dataset_id: Dataset ID
        max_entities_per_prompt: 每次提示詞中包含的最大實體數
        temperature: LLM temperature
        batch_size: 批次處理大小
    
    Returns:
        {
            'new_relations': int,  # 新增關係數量
            'processed_chunks': int,  # 處理的 Chunk 數量
            'density_before': float,  # 強化前的密度
            'density_after': float,  # 強化後的密度
            'avg_degree_before': float,  # 強化前的平均度數
            'avg_degree_after': float,  # 強化後的平均度數
        }
    """
    
    print("="*70)
    print("🔗 關係強化流程開始")
    print("="*70)
    
    # ═══════════════════════════════════════════════════════════════
    # 階段零：記錄強化前的狀態
    # ═══════════════════════════════════════════════════════════════
    with driver.session() as session:
        entity_count = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
        relation_count_before = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
        
        # 計算平均度數
        avg_degree_before = session.run("""
            MATCH (e:Entity)
            OPTIONAL MATCH (e)-[r:RELATION]-()
            WITH e, count(r) AS degree
            RETURN avg(degree) AS avg_degree
        """).single()["avg_degree"] or 0.0
        
    density_before = relation_count_before / entity_count if entity_count > 0 else 0.0
    
    print(f"\n📊 強化前狀態：")
    print(f"  • 實體節點：{entity_count:,}")
    print(f"  • 語義關係：{relation_count_before:,}")
    print(f"  • 關係密度：{density_before:.3f}")
    print(f"  • 平均度數：{avg_degree_before:.2f}")
    
    # ═══════════════════════════════════════════════════════════════
    # 階段一：檢索現有 Chunk 和 Entity
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🔍 階段一：檢索現有數據...")
    
    with driver.session() as session:
        # 檢索所有 Chunk
        chunks = session.run("""
            MATCH (c:Chunk {dataset: $dataset})
            RETURN c.id AS chunk_id, c.text AS chunk_text
            ORDER BY c.id
        """, dataset=dataset_id).data()
        
        # 檢索所有 Entity
        entities = session.run("""
            MATCH (e:Entity)
            RETURN e.name AS entity_name
            ORDER BY e.name
        """).data()
        
    entity_list = [e["entity_name"] for e in entities]
    
    print(f"  ✅ 檢索到 {len(chunks)} 個 Chunks")
    print(f"  ✅ 檢索到 {len(entity_list)} 個 Entities")
    
    # ═══════════════════════════════════════════════════════════════
    # 階段二：批次關係抽取
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🤖 階段二：LLM 關係抽取（批次處理，batch_size={batch_size}）...")
    
    all_extracted_triples = []
    processed_count = 0
    
    # 格式化實體列表（只做一次）
    formatted_entity_list = format_entity_list_for_prompt(entity_list, max_entities_per_prompt)
    
    for i in range(0, len(chunks), batch_size):
        batch_chunks = chunks[i:i+batch_size]
        
        for chunk in batch_chunks:
            chunk_id = chunk["chunk_id"]
            chunk_text = chunk["chunk_text"]
            
            # 構建提示詞
            prompt = RELATION_ENHANCEMENT_PROMPT.format(
                entity_list=formatted_entity_list,
                chunk_text=chunk_text
            )
            
            # 調用 LLM
            try:
                response = client.chat(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    options={"temperature": temperature, "top_p": 0.9},
                )
                content = response.get("message", {}).get("content", "")
                
                # 解析三元組
                triples = parse_triples(content)
                
                # 添加來源信息
                for triple in triples:
                    triple["source_chunk"] = chunk_id
                
                all_extracted_triples.extend(triples)
                processed_count += 1
                
                if processed_count % 5 == 0:
                    print(f"  ↳ 已處理 {processed_count}/{len(chunks)} Chunks，累計提取 {len(all_extracted_triples)} 個三元組")
                    
            except Exception as e:
                print(f"  ⚠️  Chunk {chunk_id} 抽取失敗：{e}")
                continue
    
    print(f"\n  ✅ 抽取完成：共 {len(all_extracted_triples)} 個候選三元組")
    
    # ═══════════════════════════════════════════════════════════════
    # 階段三：增量寫入新關係（MATCH + MERGE）
    # ═══════════════════════════════════════════════════════════════
    print(f"\n💾 階段三：增量寫入新關係（僅連接現有實體）...")
    
    new_relations_count = 0
    skipped_count = 0
    
    with driver.session() as session:
        for triple in all_extracted_triples:
            head = triple.get("head")
            relation = triple.get("relation")
            tail = triple.get("tail")
            source_chunk = triple.get("source_chunk")
            
            if not all([head, relation, tail]):
                skipped_count += 1
                continue
            
            # 關鍵：使用 MATCH + MERGE（不創建新實體）
            result = session.run("""
                // 1. 匹配現有的頭實體和尾實體
                MATCH (h:Entity {name: $head})
                MATCH (t:Entity {name: $tail})
                
                // 2. 增量合併關係（基於 head + type + tail 唯一性）
                MERGE (h)-[r:RELATION {type: $relation}]->(t)
                ON CREATE SET 
                    r.chunks = [$source_chunk],
                    r.created_at = timestamp(),
                    r.confidence = 0.95,
                    r.enhanced = true
                ON MATCH SET 
                    r.chunks = CASE 
                        WHEN $source_chunk IN r.chunks THEN r.chunks 
                        ELSE r.chunks + $source_chunk 
                    END,
                    r.last_updated = timestamp()
                
                RETURN r.enhanced AS is_new
            """, head=head, tail=tail, relation=relation, source_chunk=source_chunk)
            
            record = result.single()
            if record and record.get("is_new"):
                new_relations_count += 1
    
    print(f"  ✅ 新增關係：{new_relations_count:,}")
    print(f"  ⚠️  跳過（實體不存在或格式錯誤）：{skipped_count:,}")
    
    # ═══════════════════════════════════════════════════════════════
    # 階段四：計算強化後的狀態
    # ═══════════════════════════════════════════════════════════════
    with driver.session() as session:
        relation_count_after = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
        
        avg_degree_after = session.run("""
            MATCH (e:Entity)
            OPTIONAL MATCH (e)-[r:RELATION]-()
            WITH e, count(r) AS degree
            RETURN avg(degree) AS avg_degree
        """).single()["avg_degree"] or 0.0
    
    density_after = relation_count_after / entity_count if entity_count > 0 else 0.0
    
    print(f"\n📊 強化後狀態：")
    print(f"  • 實體節點：{entity_count:,} （無變化 ✅）")
    print(f"  • 語義關係：{relation_count_after:,} （+{relation_count_after - relation_count_before:,} ✅）")
    print(f"  • 關係密度：{density_after:.3f} （從 {density_before:.3f} 提升 {((density_after/density_before - 1) * 100):.1f}% ✅）")
    print(f"  • 平均度數：{avg_degree_after:.2f} （從 {avg_degree_before:.2f} 提升 {((avg_degree_after/avg_degree_before - 1) * 100):.1f}% ✅）")
    
    print("\n" + "="*70)
    print("✅ 關係強化流程完成！")
    print("="*70)
    
    return {
        "new_relations": new_relations_count,
        "processed_chunks": processed_count,
        "density_before": density_before,
        "density_after": density_after,
        "avg_degree_before": avg_degree_before,
        "avg_degree_after": avg_degree_after,
        "entity_count": entity_count,
        "relation_count_before": relation_count_before,
        "relation_count_after": relation_count_after,
    }



# ✅ 需要从 builder 导入 parse_triples 函数
from typing import List, Dict, Any
from ollama import Client
from src.builder import parse_triples
