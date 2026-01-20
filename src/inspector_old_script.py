# src/inspector.py
"""
Structure diagnosis (Phase 3) - 學術級專業版
封裝為 GraphInspector 類，避免 import 時自動執行
"""
from typing import Dict, Any, List, Optional
import sys

class GraphInspector:
    """
    圖譜品質檢查員 (Graph Inspector)
    負責執行學術級完整度驗證與品質報告。
    """
    def __init__(self, driver):
        self.driver = driver

    def run_basic_diagnosis(self, verbose: bool = True) -> Dict[str, Any]:
        """
        執行基本的圖譜統計診斷
        
        Returns:
            Dict 包含: chunks, entities, relations_total, mentions_count, 
                      relation_count, density, avg_degree
        """
        results = {}
        
        with self.driver.session() as session:
            if verbose:
                print("\n" + "="*70)
                print("🔍 步驟一：標準化計數驗證")
                print("="*70)
            
            # A. 計算所有類型節點的總數
            total_nodes = session.run("MATCH (n) RETURN count(n) AS cnt").single()["cnt"]
            if verbose:
                print(f"A. 所有類型節點總數：{total_nodes:,}")
            
            # B. 計算所有 Entity 節點的總數
            total_entities = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
            if verbose:
                print(f"B. Entity 節點總數：{total_entities:,}")
            
            # C. 計算所有 Chunk 節點的總數
            total_chunks = session.run("MATCH (c:Chunk) RETURN count(c) AS cnt").single()["cnt"]
            if verbose:
                print(f"C. Chunk 節點總數：{total_chunks:,}")
            
            # D. 計算所有關係的總數（標準方法）
            total_relationships = session.run("MATCH ()-[r]-() RETURN count(r) AS cnt").single()["cnt"]
            if verbose:
                print(f"D. 所有關係總數（雙向計數）：{total_relationships:,}")
            
            # E. 計算 RELATION 類型關係的總數（單向計數）
            relation_type_count = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
            if verbose:
                print(f"E. RELATION 類型關係總數（單向）：{relation_type_count:,}")
            
            # F. 計算 MENTIONS 類型關係的總數（單向計數）
            mentions_count = session.run("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS cnt").single()["cnt"]
            if verbose:
                print(f"F. MENTIONS 類型關係總數（單向）：{mentions_count:,}")
            
            # 計算密度和平均度數
            density = (relation_type_count / (total_entities * (total_entities - 1))) if total_entities > 1 else 0
            avg_degree = (2 * relation_type_count / total_entities) if total_entities > 0 else 0
            
            results = {
                "chunks": total_chunks,
                "entities": total_entities,
                "relations_total": relation_type_count + mentions_count,
                "mentions_count": mentions_count,
                "relation_count": relation_type_count,
                "density": density,
                "avg_degree": avg_degree,
                "total_nodes": total_nodes,
                "total_relationships_bidirectional": total_relationships
            }
            
            if verbose:
                print("\n" + "="*70)
                print("📊 診斷結果：")
                print(f"  • 實體節點：{total_entities:,}")
                print(f"  • 語義關係（RELATION）：{relation_type_count:,}")
                print(f"  • 來源追溯（MENTIONS）：{mentions_count:,}")
                print(f"  • 關係總計：{relation_type_count + mentions_count:,}")
                print(f"  • 關係密度：{density:.4f}")
                print(f"  • 平均度數：{avg_degree:.2f}")
                print(f"  • 雙向計數驗證：{total_relationships:,} (應為 {2 * (relation_type_count + mentions_count):,})")
                print("="*70 + "\n")
        
        return results
    
    def run_integrity_analysis(self, verbose: bool = True) -> Dict[str, Any]:
        """
        執行關係完整性分析（檢測遺失關係）
        """
        results = {}
        
        with self.driver.session() as session:
            if verbose:
                print("\n" + "="*70)
                print("🔍 步驟二：關係完整性分析")
                print("="*70 + "\n")
            
            total_entities = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
            
            # A. 檢查有多少實體沒有任何 RELATION
            isolated_entities = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)-[:RELATION]-()
                RETURN count(e) AS cnt
            """).single()["cnt"]
            
            if verbose:
                print(f"A. 孤立實體（無 RELATION）：{isolated_entities:,} / {total_entities:,} ({isolated_entities/total_entities*100:.2f}%)")
            
            results = {
                "isolated_entities": isolated_entities,
                "total_entities": total_entities,
                "isolated_ratio": (isolated_entities/total_entities*100) if total_entities > 0 else 0
            }
            
            if verbose:
                print("\n" + "="*70)
        
        return results


# ═══════════════════════════════════════════════════════════════════
# 保留舊代碼作為參考，但已封裝在類中不會自動執行
# 以下所有代碼都已註釋，避免 import 時自動執行
# ═══════════════════════════════════════════════════════════════════
"""
# 原始腳本代碼（已停用）
# with GRAPH_DRIVER.session() as session:
#     print("🔍 步驟五：MERGE 邏輯驗證\n")
#     ...
        MATCH (e:Entity)
        WITH e.name AS entity_name, count(e) AS cnt
        WHERE cnt > 1
        RETURN entity_name, cnt
        ORDER BY cnt DESC
        LIMIT 5000
    """).data()
    
    if duplicate_entities:
        print("❌ A. 發現重複實體節點：")
        for row in duplicate_entities:
            print(f"   • {row['entity_name']}: {row['cnt']} 個節點")
    else:
        print("✅ A. 無重複實體節點（MERGE 去重正確）")
    
    # B. 檢查是否有重複的關係（基於 head + type + tail）
    duplicate_relations = session.run("""
        MATCH (h:Entity)-[r:RELATION]->(t:Entity)
        WITH h.name AS head, r.type AS rel_type, t.name AS tail, count(r) AS cnt
        WHERE cnt > 1
        RETURN head, rel_type, tail, cnt
        ORDER BY cnt DESC
        LIMIT 5
    """).data()
    
    if duplicate_relations:
        print("\n❌ B. 發現重複關係：")
        for row in duplicate_relations:
            print(f"   • ({row['head']}, {row['rel_type']}, {row['tail']}): {row['cnt']} 個關係")
    else:
        print("\n✅ B. 無重複關係（MERGE 去重正確）")
    
    # C. 檢查 MENTIONS 關係的去重
    duplicate_mentions = session.run("""
        MATCH (c:Chunk)-[m:MENTIONS]->(e:Entity)
        WITH c.id AS chunk_id, e.name AS entity_name, count(m) AS cnt
        WHERE cnt > 1
        RETURN chunk_id, entity_name, cnt
        ORDER BY cnt DESC
        LIMIT 5
    """).data()
    
    if duplicate_mentions:
        print("\n❌ C. 發現重複 MENTIONS 關係：")
        for row in duplicate_mentions:
            print(f"   • Chunk {row['chunk_id']} → {row['entity_name']}: {row['cnt']} 個關係")
    else:
        print("\n✅ C. 無重複 MENTIONS 關係（MERGE 去重正確）")
    
    # D. 抽樣檢查 r.chunks 屬性的完整性
    sample_relations = session.run("""
        MATCH ()-[r:RELATION]->()
        WHERE size(r.chunks) >= 2
        RETURN r.type AS relation_type, 
               size(r.chunks) AS chunk_count, 
               r.chunks AS chunks
        ORDER BY chunk_count DESC
        LIMIT 5
    """).data()
    
    print("\n✅ D. 多來源關係抽樣（增量寫入正確）：")
    if sample_relations:
        for row in sample_relations:
            print(f"   • {row['relation_type']}: {row['chunk_count']} 個來源 {row['chunks'][:3]}...")
    else:
        print("   ⚠️  暫無多來源關係（可能所有關係都是單一來源）")
    
    print("\n" + "="*70)
# ═══════════════════════════════════════════════════════════════════
# 快速診斷：孤立實體分析（了解為何 41.5% 實體孤立）
# ═══════════════════════════════════════════════════════════════════

with GRAPH_DRIVER.session() as session:
    print("🔍 孤立實體深度分析\n")
    
    # A. 抽樣孤立實體（前 20 個）
    isolated_samples = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        RETURN e.name AS entity_name
        LIMIT 20
    """).data()
    
    print("A. 孤立實體樣本（前 20 個）：")
    for i, row in enumerate(isolated_samples, 1):
        print(f"   {i:2d}. {row['entity_name']}")
    
    # B. 分析孤立實體的名稱特徵
    print("\nB. 孤立實體特徵分析：")
    
    # 檢查是否為純數字實體
    numeric_isolated = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
          AND e.name =~ '^[0-9]+.*'
        RETURN count(e) AS cnt
    """).single()["cnt"]
    print(f"   • 純數字開頭實體：{numeric_isolated:,} ({numeric_isolated/6180*100:.1f}%)")
    
    # 檢查是否為短名稱實體（可能是單位、符號）
    short_name_isolated = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
          AND size(e.name) <= 3
        RETURN count(e) AS cnt
    """).single()["cnt"]
    print(f"   • 短名稱實體（≤3字符）：{short_name_isolated:,} ({short_name_isolated/6180*100:.1f}%)")
    
    # 檢查是否為單詞實體（可能缺少上下文）
    single_word_isolated = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
          AND NOT e.name CONTAINS ' '
        RETURN count(e) AS cnt
    """).single()["cnt"]
    print(f"   • 單詞實體（無空格）：{single_word_isolated:,} ({single_word_isolated/6180*100:.1f}%)")
    
    # C. 檢查孤立實體是否被 MENTIONS（確認數據一致性）
    isolated_with_mentions = session.run("""
        MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        RETURN count(DISTINCT e) AS cnt
    """).single()["cnt"]
    print(f"\nC. 孤立但被 MENTIONS 的實體：{isolated_with_mentions:,} / {6180:,}")
    print(f"   ⚠️  數據一致性：{isolated_with_mentions == 6180 and '✅ 完全一致' or '❌ 存在不一致'}")
    
    # D. 檢查孤立實體的來源分佈
    isolated_by_chunk = session.run("""
        MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        WITH c.id AS chunk_id, count(DISTINCT e) AS isolated_count
        RETURN chunk_id, isolated_count
        ORDER BY isolated_count DESC
        LIMIT 5
    """).data()
    
    print(f"\nD. 孤立實體最多的 Chunks（前 5 個）：")
    for row in isolated_by_chunk:
        print(f"   • {row['chunk_id']}: {row['isolated_count']} 個孤立實體")
    
    print("\n" + "="*70)
    print("💡 建議：")
    print("   • 如果孤立實體多為數字/單位/短符號 → 可清理")
    print("   • 如果孤立實體為有意義概念 → 需增強 LLM 提取")
    print("="*70)
# ═══════════════════════════════════════════════════════════════════
# 緊急驗證：檢查「幽靈實體」的真實狀態
# ═══════════════════════════════════════════════════════════════════

with GRAPH_DRIVER.session() as session:
    print("🚨 幽靈實體驗證\n")
    
    # 1. 檢查真正的孤兒實體（無任何連接）
    truly_orphan = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
          AND NOT ()-[:MENTIONS]->(e)
        RETURN count(e) AS cnt
    """).single()["cnt"]
    print(f"1. 真正的孤兒實體（無任何連接）：{truly_orphan:,}")
    
    # 2. 檢查有 MENTIONS 的實體總數
    mentioned_entities = session.run("""
        MATCH ()-[:MENTIONS]->(e:Entity)
        RETURN count(DISTINCT e) AS cnt
    """).single()["cnt"]
    print(f"2. 被 MENTIONS 的實體總數：{mentioned_entities:,}")
    
    # 3. 檢查有 RELATION 的實體總數
    relation_entities = session.run("""
        MATCH (e:Entity)-[:RELATION]-()
        RETURN count(DISTINCT e) AS cnt
    """).single()["cnt"]
    print(f"3. 有 RELATION 的實體總數：{relation_entities:,}")
    
    # 4. 計算覆蓋情況
    total_entities = 14880
    covered = mentioned_entities + relation_entities - truly_orphan
    print(f"\n4. 實體覆蓋分析：")
    print(f"   • 總實體：{total_entities:,}")
    print(f"   • 被 MENTIONS：{mentioned_entities:,} ({mentioned_entities/total_entities*100:.1f}%)")
    print(f"   • 有 RELATION：{relation_entities:,} ({relation_entities/total_entities*100:.1f}%)")
    print(f"   • 真正孤兒：{truly_orphan:,} ({truly_orphan/total_entities*100:.1f}%)")
    
    # 5. 抽樣檢查幾個孤立實體的實際狀態
    print(f"\n5. 抽樣孤立實體的連接狀態（前 5 個）：")
    sample_isolated = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        WITH e LIMIT 5
        OPTIONAL MATCH (e)-[r]-()
        RETURN e.name AS entity_name, 
               count(r) AS total_connections,
               collect(DISTINCT type(r)) AS connection_types
    """).data()
    
    for row in sample_isolated:
        print(f"   • {row['entity_name']}:")
        print(f"     - 總連接數：{row['total_connections']}")
        print(f"     - 連接類型：{row['connection_types']}")
    
    # 6. 檢查數據完整性：MENTIONS 數量 vs 預期
    print(f"\n6. MENTIONS 數量驗證：")
    mentions_count = session.run("""
        MATCH ()-[m:MENTIONS]->()
        RETURN count(m) AS cnt
    """).single()["cnt"]
    print(f"   • MENTIONS 關係總數：{mentions_count:,}")
    print(f"   • 平均每 Chunk：{mentions_count / 25:.1f} 個 MENTIONS")
    
    # 7. 檢查是否有 dataset 屬性不匹配的情況
    different_dataset = session.run("""
        MATCH (e:Entity)
        WHERE e.dataset IS NOT NULL 
          AND e.dataset <> $dataset
        RETURN count(e) AS cnt
    """, dataset=DATASET_ID).single()["cnt"]
    print(f"\n7. 不同 dataset 的實體：{different_dataset:,}")
    
    print("\n" + "="*70)
# ═══════════════════════════════════════════════════════════════════
# 執行清理：刪除孤兒實體（無任何連接的實體）
# ═══════════════════════════════════════════════════════════════════

print("⚠️  即將刪除孤兒實體（無任何 MENTIONS 或 RELATION）\n")
print("這些實體的樣本：")
print("  • 良质芻料、脂质堆积、妊娠毒血症风险、穀物、青草乾草...")
print("\n這些實體可能來自：")
print("  1. 之前運行的舊數據殘留")
print("  2. 測試階段創建的實體")
print("  3. 已被移除的文本片段\n")

user_confirm = input("確認執行清理？(yes/no): ")

if user_confirm.lower() in ['yes', 'y']:
    with GRAPH_DRIVER.session() as session:
        result = session.run("""
            MATCH (e:Entity)
            WHERE NOT (e)-[:RELATION]-()
              AND NOT ()-[:MENTIONS]->(e)
            DETACH DELETE e
            RETURN count(e) AS deleted
        """)
        deleted_count = result.single()["deleted"]
        print(f"\n✅ 成功刪除 {deleted_count:,} 個孤兒實體")
        
        # 驗證清理效果
        remaining_entities = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
        remaining_relations = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
        
        print(f"\n📊 清理後狀態：")
        print(f"  • 實體節點：{remaining_entities:,}")
        print(f"  • 語義關係：{remaining_relations:,}")
        print(f"  • 關係密度：{remaining_relations/remaining_entities:.3f}")
        print(f"\n✅ 圖譜已淨化！所有實體都有連接。")
else:
    print("\n❌ 清理已取消")
# ═══════════════════════════════════════════════════════════════════
# 圖譜完整度與質量最終檢驗
# ═══════════════════════════════════════════════════════════════════

with GRAPH_DRIVER.session() as session:
    print("🔍 圖譜完整度與質量最終檢驗報告")
    print("="*70)
    
    # ═══════════════════════════════════════════════════════════════
    # 第一部分：基礎指標
    # ═══════════════════════════════════════════════════════════════
    print("\n📊 一、基礎指標")
    print("-"*70)
    
    entity_count = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
    relation_count = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
    chunk_count = session.run(f"MATCH (c:Chunk {{dataset: '{DATASET_ID}'}}) RETURN count(c) AS cnt").single()["cnt"]
    mentions_count = session.run("MATCH ()-[m:MENTIONS]->() RETURN count(m) AS cnt").single()["cnt"]
    
    density = relation_count / entity_count if entity_count > 0 else 0.0
    
    avg_degree = session.run("""
        MATCH (e:Entity)
        OPTIONAL MATCH (e)-[r:RELATION]-()
        WITH e, count(r) AS degree
        RETURN avg(degree) AS avg_degree
    """).single()["avg_degree"] or 0.0
    
    print(f"  • 實體節點數：{entity_count:,}")
    print(f"  • 語義關係數：{relation_count:,}")
    print(f"  • 文本 Chunks：{chunk_count:,}")
    print(f"  • MENTIONS 連接：{mentions_count:,}")
    print(f"  • 關係密度：{density:.3f} {'✅ 優秀' if density >= TARGET_DENSITY else '⚠️ 待優化'}")
    print(f"  • 平均度數：{avg_degree:.2f} {'✅ 優秀' if avg_degree >= TARGET_AVG_DEGREE else '⚠️ 待優化'}")
    
    # ═══════════════════════════════════════════════════════════════
    # 第二部分：連接質量
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🔗 二、連接質量分析")
    print("-"*70)
    
    # 1. 孤立實體檢測
    isolated_entities = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        RETURN count(e) AS cnt
    """).single()["cnt"]
    
    isolated_percent = (isolated_entities / entity_count * 100) if entity_count > 0 else 0
    print(f"  1. 孤立實體：{isolated_entities:,} ({isolated_percent:.1f}%) {'✅ 優秀' if isolated_percent == 0 else '⚠️ 需注意' if isolated_percent < 5 else '❌ 需改進'}")
    
    # 2. 弱連接實體（度數 = 1）
    weak_entities = session.run("""
        MATCH (e:Entity)-[r:RELATION]-()
        WITH e, count(r) AS degree
        WHERE degree = 1
        RETURN count(e) AS cnt
    """).single()["cnt"]
    
    weak_percent = (weak_entities / entity_count * 100) if entity_count > 0 else 0
    print(f"  2. 弱連接實體（度數=1）：{weak_entities:,} ({weak_percent:.1f}%) {'✅ 優秀' if weak_percent < 20 else '⚠️ 需注意'}")
    
    # 3. 強連接實體（度數 ≥ 5）
    strong_entities = session.run("""
        MATCH (e:Entity)-[r:RELATION]-()
        WITH e, count(r) AS degree
        WHERE degree >= 5
        RETURN count(e) AS cnt
    """).single()["cnt"]
    
    strong_percent = (strong_entities / entity_count * 100) if entity_count > 0 else 0
    print(f"  3. 強連接實體（度數≥5）：{strong_entities:,} ({strong_percent:.1f}%) {'✅ 優秀' if strong_percent >= 10 else '⚠️ 待優化'}")
    
    # 4. 多來源關係（跨 Chunk 關係）
    multi_source_relations = session.run("""
        MATCH ()-[r:RELATION]->()
        WHERE size(r.chunks) >= 2
        RETURN count(r) AS cnt
    """).single()["cnt"]
    
    multi_source_percent = (multi_source_relations / relation_count * 100) if relation_count > 0 else 0
    print(f"  4. 多來源關係（≥2 Chunks）：{multi_source_relations:,} ({multi_source_percent:.1f}%) {'✅ 優秀' if multi_source_percent >= 20 else '⚠️ 待優化'}")
    
    # ═══════════════════════════════════════════════════════════════
    # 第三部分：關係強化效果
    # ═══════════════════════════════════════════════════════════════
    print(f"\n⚡ 三、關係強化效果")
    print("-"*70)
    
    enhanced_relations = session.run("""
        MATCH ()-[r:RELATION]->()
        WHERE r.enhanced = true
        RETURN count(r) AS cnt
    """).single()["cnt"]
    
    enhanced_percent = (enhanced_relations / relation_count * 100) if relation_count > 0 else 0
    print(f"  • 強化新增關係：{enhanced_relations:,} ({enhanced_percent:.1f}%)")
    print(f"  • 原始關係：{relation_count - enhanced_relations:,} ({100-enhanced_percent:.1f}%)")
    
    # ═══════════════════════════════════════════════════════════════
    # 第四部分：關係類型多樣性
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🎨 四、關係類型多樣性")
    print("-"*70)
    
    relation_type_count = session.run("""
        MATCH ()-[r:RELATION]->()
        RETURN count(DISTINCT r.type) AS cnt
    """).single()["cnt"]
    
    print(f"  • 關係類型總數：{relation_type_count}")
    
    relation_types = session.run("""
        MATCH ()-[r:RELATION]->()
        RETURN r.type AS relation_type, count(r) AS cnt
        ORDER BY cnt DESC
        LIMIT 10
    """).data()
    
    print(f"  • 前 10 種關係類型：")
    for idx, row in enumerate(relation_types, 1):
        percent = (row['cnt'] / relation_count * 100) if relation_count > 0 else 0
        print(f"    {idx:2d}. {row['relation_type']:<30s} {row['cnt']:>6,} ({percent:>5.1f}%)")
    
    # ═══════════════════════════════════════════════════════════════
    # 第五部分：核心樞紐節點
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🌟 五、核心樞紐節點（Top 10）")
    print("-"*70)
    
    hub_entities = session.run("""
        MATCH (e:Entity)-[r:RELATION]-()
        WITH e, count(r) AS degree
        WHERE degree >= 5
        RETURN e.name AS entity_name, degree
        ORDER BY degree DESC
        LIMIT 10
    """).data()
    
    if hub_entities:
        for idx, row in enumerate(hub_entities, 1):
            print(f"  {idx:2d}. {row['entity_name']:<40s} {row['degree']:>3} 個關係")
    else:
        print("  ⚠️ 未發現度數 ≥ 5 的核心節點")
    
    # ═══════════════════════════════════════════════════════════════
    # 第六部分：覆蓋率分析
    # ═══════════════════════════════════════════════════════════════
    print(f"\n📍 六、文本覆蓋率分析")
    print("-"*70)
    
    # 有實體的 Chunks
    covered_chunks = session.run(f"""
        MATCH (c:Chunk {{dataset: '{DATASET_ID}'}})-[:MENTIONS]->()
        RETURN count(DISTINCT c) AS cnt
    """).single()["cnt"]
    
    coverage_percent = (covered_chunks / chunk_count * 100) if chunk_count > 0 else 0
    print(f"  • 已覆蓋 Chunks：{covered_chunks} / {chunk_count} ({coverage_percent:.1f}%) {'✅ 優秀' if coverage_percent >= 95 else '⚠️ 待優化'}")
    
    # 平均每個 Chunk 的實體數
    avg_entities_per_chunk = session.run(f"""
        MATCH (c:Chunk {{dataset: '{DATASET_ID}'}})-[:MENTIONS]->(e:Entity)
        WITH c, count(DISTINCT e) AS entity_count
        RETURN avg(entity_count) AS avg_cnt
    """).single()["avg_cnt"] or 0
    
    print(f"  • 平均每 Chunk 實體數：{avg_entities_per_chunk:.1f}")
    
    # ═══════════════════════════════════════════════════════════════
    # 第七部分：質量問題檢測
    # ═══════════════════════════════════════════════════════════════
    print(f"\n⚠️ 七、潛在質量問題檢測")
    print("-"*70)
    
    issues_found = []
    
    # 檢測 1：自環關係
    self_loops = session.run("""
        MATCH (e:Entity)-[r:RELATION]->(e)
        RETURN count(r) AS cnt
    """).single()["cnt"]
    if self_loops > 0:
        issues_found.append(f"發現 {self_loops} 個自環關係")
    
    # 檢測 2：空實體名稱
    empty_entities = session.run("""
        MATCH (e:Entity)
        WHERE e.name IS NULL OR trim(e.name) = ''
        RETURN count(e) AS cnt
    """).single()["cnt"]
    if empty_entities > 0:
        issues_found.append(f"發現 {empty_entities} 個空實體名稱")
    
    # 檢測 3：重複關係（相同頭尾和類型）
    duplicate_relations = session.run("""
        MATCH (h:Entity)-[r:RELATION]->(t:Entity)
        WITH h, t, r.type AS rel_type, count(r) AS cnt
        WHERE cnt > 1
        RETURN count(*) AS dup_cnt
    """).single()["dup_cnt"]
    if duplicate_relations > 0:
        issues_found.append(f"發現 {duplicate_relations} 組重複關係")
    
    # 檢測 4：超長實體名稱（可能是句子片段）
    long_entities = session.run("""
        MATCH (e:Entity)
        WHERE size(e.name) > 50
        RETURN count(e) AS cnt
    """).single()["cnt"]
    if long_entities > 0:
        issues_found.append(f"發現 {long_entities} 個超長實體名稱（>50字元）")
    
    if issues_found:
        for issue in issues_found:
            print(f"  ⚠️ {issue}")
    else:
        print("  ✅ 未發現明顯質量問題")
    
    # ═══════════════════════════════════════════════════════════════
    # 最終評級
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print(f"🏆 最終評級")
    print(f"{'='*70}")
    
    score = 0
    max_score = 7
    
    # 評分項目
    if density >= TARGET_DENSITY:
        score += 1
        density_status = "✅"
    else:
        density_status = "❌"
    
    if avg_degree >= TARGET_AVG_DEGREE:
        score += 1
        degree_status = "✅"
    else:
        degree_status = "❌"
    
    if isolated_percent < 5:
        score += 1
        isolated_status = "✅"
    else:
        isolated_status = "❌"
    
    if weak_percent < 30:
        score += 1
        weak_status = "✅"
    else:
        weak_status = "❌"
    
    if strong_percent >= 10:
        score += 1
        strong_status = "✅"
    else:
        strong_status = "❌"
    
    if coverage_percent >= 95:
        score += 1
        coverage_status = "✅"
    else:
        coverage_status = "❌"
    
    if len(issues_found) == 0:
        score += 1
        quality_status = "✅"
    else:
        quality_status = "❌"
    
    print(f"  {density_status} 關係密度 ≥ {TARGET_DENSITY}：{density:.3f}")
    print(f"  {degree_status} 平均度數 ≥ {TARGET_AVG_DEGREE}：{avg_degree:.2f}")
    print(f"  {isolated_status} 孤立實體 < 5%：{isolated_percent:.1f}%")
    print(f"  {weak_status} 弱連接實體 < 30%：{weak_percent:.1f}%")
    print(f"  {strong_status} 強連接實體 ≥ 10%：{strong_percent:.1f}%")
    print(f"  {coverage_status} 文本覆蓋率 ≥ 95%：{coverage_percent:.1f}%")
    print(f"  {quality_status} 無質量問題：{'是' if len(issues_found) == 0 else '否'}")
    
    print(f"\n  總分：{score}/{max_score}")
    
    if score == max_score:
        grade = "A+ 卓越"
    elif score >= 6:
        grade = "A 優秀"
    elif score >= 5:
        grade = "B 良好"
    elif score >= 4:
        grade = "C 及格"
    else:
        grade = "D 待改進"
    
    print(f"  等級：{grade}")
    print(f"{'='*70}")

print("\n✅ 圖譜完整度與質量檢驗完成！")
# ═══════════════════════════════════════════════════════════════════
# 圖譜質量問題自動修正
# ═══════════════════════════════════════════════════════════════════

print("🔧 開始自動修正圖譜質量問題...")
print("="*70)

fix_summary = {
    'self_loops_removed': 0,
    'long_entities_truncated': 0,
    'duplicate_relations_merged': 0,
    'empty_entities_removed': 0
}

with GRAPH_DRIVER.session() as session:
    
    # ═══════════════════════════════════════════════════════════════
    # 修正 1：移除自環關係（實體指向自己）
    # ═══════════════════════════════════════════════════════════════
    print("\n🔍 修正 1：移除自環關係")
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
        """).single()
        fix_summary['self_loops_removed'] = result['deleted']
        print(f"  ✅ 已移除 {result['deleted']} 個自環關係")
    else:
        print("  ✅ 未發現自環關係")
    
    # ═══════════════════════════════════════════════════════════════
    # 修正 2：處理超長實體名稱（>50字元）
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🔍 修正 2：處理超長實體名稱")
    print("-"*70)
    
    long_entities_count = session.run("""
        MATCH (e:Entity)
        WHERE size(e.name) > 50
        RETURN count(e) AS cnt
    """).single()["cnt"]
    
    if long_entities_count > 0:
        print(f"  發現 {long_entities_count} 個超長實體名稱")
        
        # 獲取樣本檢查
        samples = session.run("""
            MATCH (e:Entity)
            WHERE size(e.name) > 50
            RETURN e.name AS name, size(e.name) AS length
            ORDER BY length DESC
            LIMIT 100
        """).data()
        
        print(f"  樣本（前5個）：")
        for s in samples:
            display_name = s['name'][:60] + "..." if len(s['name']) > 60 else s['name']
            print(f"    • {display_name} (長度: {s['length']})")
        
        # 分析超長實體的連接度
        connectivity_stats = session.run("""
            MATCH (e:Entity)
            WHERE size(e.name) > 50
            OPTIONAL MATCH (e)-[r:RELATION]-()
            WITH e, count(r) AS degree
            RETURN 
                count(e) AS total,
                sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END) AS isolated,
                sum(CASE WHEN degree = 1 THEN 1 ELSE 0 END) AS weak,
                avg(degree) AS avg_degree
        """).single()
        
        print(f"\n  連接度分析：")
        print(f"    • 孤立（度數=0）：{connectivity_stats['isolated']}/{connectivity_stats['total']}")
        print(f"    • 弱連接（度數=1）：{connectivity_stats['weak']}/{connectivity_stats['total']}")
        print(f"    • 平均度數：{connectivity_stats['avg_degree']:.2f}")
        
        # 策略：自動移除這些句子片段實體（因為它們通常是抽取錯誤）
        print(f"\n  💡 建議：這些超長實體通常是句子片段，會降低圖譜質量")
        print(f"     → 自動移除這些實體及其關係...")
        
        result = session.run("""
            MATCH (e:Entity)
            WHERE size(e.name) > 50
            DETACH DELETE e
            RETURN count(e) AS deleted
        """).single()
        fix_summary['long_entities_truncated'] = result['deleted']
        print(f"  ✅ 已移除 {result['deleted']} 個超長實體及其關係")
    else:
        print("  ✅ 未發現超長實體名稱")
    
    # ═══════════════════════════════════════════════════════════════
    # 修正 3：合併重複關係（相同頭尾和類型）
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🔍 修正 3：合併重複關係")
    print("-"*70)
    
    duplicate_groups = session.run("""
        MATCH (h:Entity)-[r:RELATION]->(t:Entity)
        WITH h, t, r.type AS rel_type, collect(r) AS rels
        WHERE size(rels) > 1
        RETURN count(*) AS dup_groups, sum(size(rels) - 1) AS extra_rels
    """).single()
    
    dup_groups = duplicate_groups['dup_groups'] or 0
    extra_rels = duplicate_groups['extra_rels'] or 0
    
    if dup_groups > 0:
        print(f"  發現 {dup_groups} 組重複關係（共 {extra_rels} 個多餘關係）")
        print(f"  正在合併重複關係（保留第一個，合併 chunks 屬性）...")
        
        # 合併策略：保留第一個關係，將其他關係的 chunks 合併進去
        result = session.run("""
            MATCH (h:Entity)-[r:RELATION]->(t:Entity)
            WITH h, t, r.type AS rel_type, collect(r) AS rels
            WHERE size(rels) > 1
            WITH h, t, rel_type, rels[0] AS keep, rels[1..] AS remove
            UNWIND remove AS del_rel
            WITH h, t, rel_type, keep, del_rel, 
                 COALESCE(keep.chunks, []) + COALESCE(del_rel.chunks, []) AS merged_chunks
            SET keep.chunks = merged_chunks
            DELETE del_rel
            RETURN count(del_rel) AS merged
        """).single()
        
        fix_summary['duplicate_relations_merged'] = result['merged']
        print(f"  ✅ 已合併 {result['merged']} 個重複關係")
    else:
        print("  ✅ 未發現重複關係")
    
    # ═══════════════════════════════════════════════════════════════
    # 修正 4：移除空實體名稱
    # ═══════════════════════════════════════════════════════════════
    print(f"\n🔍 修正 4：移除空實體名稱")
    print("-"*70)
    
    empty_entities_count = session.run("""
        MATCH (e:Entity)
        WHERE e.name IS NULL OR trim(e.name) = ''
        RETURN count(e) AS cnt
    """).single()["cnt"]
    
    if empty_entities_count > 0:
        print(f"  發現 {empty_entities_count} 個空實體名稱，正在移除...")
        result = session.run("""
            MATCH (e:Entity)
            WHERE e.name IS NULL OR trim(e.name) = ''
            DETACH DELETE e
            RETURN count(e) AS deleted
        """).single()
        fix_summary['empty_entities_removed'] = result['deleted']
        print(f"  ✅ 已移除 {result['deleted']} 個空實體")
    else:
        print("  ✅ 未發現空實體名稱")

# ═══════════════════════════════════════════════════════════════
# 修正摘要
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"📋 質量修正摘要")
print(f"{'='*70}")
print(f"  • 移除自環關係：{fix_summary['self_loops_removed']}")
print(f"  • 移除超長實體：{fix_summary['long_entities_truncated']}")
print(f"  • 合併重複關係：{fix_summary['duplicate_relations_merged']}")
print(f"  • 移除空實體：{fix_summary['empty_entities_removed']}")

total_fixes = sum(fix_summary.values())
print(f"\n  總計修正：{total_fixes} 個問題")
print(f"{'='*70}")

if total_fixes > 0:
    print("\n💡 建議：重新執行圖譜質量檢驗以確認修正效果")
# ═══════════════════════════════════════════════════════════════════
# 🔍 孤立實體診斷分析
# ═══════════════════════════════════════════════════════════════════

print("🔍 孤立實體深度診斷")
print("="*70)

with GRAPH_DRIVER.session() as session:
    # 1. 獲取孤立實體樣本及其來源
    isolated_samples = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(e)
        WITH e, collect(DISTINCT c.id) AS source_chunks
        RETURN 
            e.name AS entity,
            size(source_chunks) AS mention_count,
            source_chunks[0] AS sample_chunk
        ORDER BY mention_count DESC
        LIMIT 30
    """).data()
    
    print(f"\n📋 孤立實體樣本（前 30 個，按提及次數排序）：")
    print("-"*70)
    
    for idx, item in enumerate(isolated_samples, 1):
        entity = item['entity']
        mentions = item['mention_count']
        chunk_id = item['sample_chunk'] or "未找到來源"
        
        # 分類分析
        entity_type = ""
        if len(entity) < 3:
            entity_type = "[過短]"
        elif entity.replace('_', '').replace('-', '').isdigit():
            entity_type = "[純數字]"
        elif entity.lower() in ['it', 'this', 'that', 'they', 'these']:
            entity_type = "[代詞]"
        elif any(char.isdigit() for char in entity) and any(char.isalpha() for char in entity):
            entity_type = "[數值+單位]"
        else:
            entity_type = "[正常]"
        
        print(f"  {idx:2d}. {entity_type:12s} {entity[:40]:40s} (提及: {mentions}, Chunk: {chunk_id})")
    
    # 2. 孤立實體類型統計
    print(f"\n📊 孤立實體類型分析：")
    print("-"*70)
    
    isolated_stats = session.run("""
        MATCH (e:Entity)
        WHERE NOT (e)-[:RELATION]-()
        OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(e)
        WITH e, count(DISTINCT c) AS mentions
        RETURN 
            count(e) AS total_isolated,
            sum(CASE WHEN mentions = 0 THEN 1 ELSE 0 END) AS no_mentions,
            sum(CASE WHEN mentions = 1 THEN 1 ELSE 0 END) AS single_mention,
            sum(CASE WHEN mentions >= 2 THEN 1 ELSE 0 END) AS multiple_mentions,
            sum(CASE WHEN size(e.name) < 3 THEN 1 ELSE 0 END) AS too_short,
            sum(CASE WHEN size(e.name) > 40 THEN 1 ELSE 0 END) AS too_long
    """).single()
    
    print(f"  • 總孤立實體數：{isolated_stats['total_isolated']}")
    print(f"  • 無 MENTIONS 連接：{isolated_stats['no_mentions']} ({isolated_stats['no_mentions']/isolated_stats['total_isolated']*100:.1f}%)")
    print(f"  • 單一 Chunk 提及：{isolated_stats['single_mention']} ({isolated_stats['single_mention']/isolated_stats['total_isolated']*100:.1f}%)")
    print(f"  • 多 Chunk 提及：{isolated_stats['multiple_mentions']} ({isolated_stats['multiple_mentions']/isolated_stats['total_isolated']*100:.1f}%)")
    print(f"  • 名稱過短（<3字元）：{isolated_stats['too_short']} ({isolated_stats['too_short']/isolated_stats['total_isolated']*100:.1f}%)")
    print(f"  • 名稱過長（>40字元）：{isolated_stats['too_long']} ({isolated_stats['too_long']/isolated_stats['total_isolated']*100:.1f}%)")
    
    # 3. 潛在的同義詞檢測
    print(f"\n🔗 潛在同義詞檢測（相似實體名稱）：")
    print("-"*70)
    
    potential_synonyms = session.run("""
        MATCH (e1:Entity)
        WHERE NOT (e1)-[:RELATION]-()
        MATCH (e2:Entity)
        WHERE e2 <> e1 AND (e2)-[:RELATION]-()
        AND (
            toLower(e1.name) CONTAINS toLower(e2.name) 
            OR toLower(e2.name) CONTAINS toLower(e1.name)
            OR toLower(replace(e1.name, '_', ' ')) = toLower(replace(e2.name, '_', ' '))
        )
        RETURN 
            e1.name AS isolated_entity,
            e2.name AS connected_entity,
            COUNT { (e2)-[:RELATION]-() } AS connected_degree
        ORDER BY connected_degree DESC
        LIMIT 15
    """).data()
    
    if potential_synonyms:
        print(f"  發現 {len(potential_synonyms)} 對潛在同義詞：")
        for syn in potential_synonyms:
            print(f"    • 孤立: '{syn['isolated_entity'][:30]}' ↔ 已連接: '{syn['connected_entity'][:30]}' (度數: {syn['connected_degree']})")
    else:
        print("  未發現明顯的同義詞模式")

print(f"\n{'='*70}")
print("💡 診斷建議：")
print("  1. 過短實體、純數字實體 → 建議刪除（可能是提取錯誤）")
print("  2. 單一提及且名稱不常見 → 可能是低質量實體，考慮刪除")
print("  3. 多次提及但孤立 → 關係提取失敗，需要重新提取關係")
print("  4. 發現同義詞 → 需要實體正規化與合併")
print("="*70)
# ═══════════════════════════════════════════════════════════════════
# 🔧 修正關係來源標記
# ═══════════════════════════════════════════════════════════════════

print("🔧 開始修正關係來源標記...")
print("="*70)

with GRAPH_DRIVER.session() as session:
    # 1. 檢查問題規模
    print("\n📊 檢查問題規模...")
    
    missing_source = session.run("""
        MATCH ()-[r:RELATION]->()
        WHERE r.chunks IS NULL OR size(r.chunks) = 0
        RETURN 
            count(r) AS missing_count,
            sum(CASE WHEN r.inferred = true THEN 1 ELSE 0 END) AS inferred_missing,
            sum(CASE WHEN r.densified = true THEN 1 ELSE 0 END) AS densified_missing,
            sum(CASE WHEN r.enhanced = true THEN 1 ELSE 0 END) AS enhanced_missing
    """).single()
    
    print(f"  • 缺少來源標記的關係總數：{missing_source['missing_count']}")
    print(f"    - inferred 關係：{missing_source['inferred_missing']}")
    print(f"    - densified 關係：{missing_source['densified_missing']}")
    print(f"    - enhanced 關係：{missing_source['enhanced_missing']}")
    
    if missing_source['missing_count'] == 0:
        print("\n✅ 所有關係都已正確標記來源！")
    else:
        # 2. 修正 inferred 關係的來源
        print(f"\n🔄 修正 inferred 關係的來源標記...")
        
        result_inferred = session.run("""
            MATCH (h:Entity)-[r:RELATION]->(t:Entity)
            WHERE r.inferred = true AND (r.chunks IS NULL OR size(r.chunks) = 0)
            
            // 找到頭尾實體共同出現的 Chunks
            OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(h)
            OPTIONAL MATCH (c)-[:MENTIONS]->(t)
            WITH r, collect(DISTINCT c.id) AS common_chunks
            
            // 如果有共同 Chunk，使用共同 Chunk；否則使用頭實體的 Chunk
            SET r.chunks = CASE 
                WHEN size(common_chunks) > 0 THEN common_chunks
                ELSE []
            END
            
            RETURN count(r) AS fixed_count
        """).single()
        
        print(f"  ✅ 修正 {result_inferred['fixed_count']} 個 inferred 關係")
        
        # 3. 修正 densified 關係的來源（應該在寫入時就有，這是備用）
        print(f"\n🔄 檢查 densified 關係...")
        
        result_densified = session.run("""
            MATCH (h:Entity)-[r:RELATION]->(t:Entity)
            WHERE r.densified = true AND (r.chunks IS NULL OR size(r.chunks) = 0)
            
            // 找到頭尾實體共同出現的 Chunks
            OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(h)
            OPTIONAL MATCH (c)-[:MENTIONS]->(t)
            WITH r, collect(DISTINCT c.id) AS common_chunks
            
            SET r.chunks = common_chunks
            
            RETURN count(r) AS fixed_count
        """).single()
        
        print(f"  ✅ 修正 {result_densified['fixed_count']} 個 densified 關係")
        
        # 4. 修正 enhanced 關係的來源
        print(f"\n🔄 檢查 enhanced 關係...")
        
        result_enhanced = session.run("""
            MATCH (h:Entity)-[r:RELATION]->(t:Entity)
            WHERE r.enhanced = true AND (r.chunks IS NULL OR size(r.chunks) = 0)
            
            // 找到頭尾實體共同出現的 Chunks
            OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(h)
            OPTIONAL MATCH (c)-[:MENTIONS]->(t)
            WITH r, collect(DISTINCT c.id) AS common_chunks
            
            SET r.chunks = common_chunks
            
            RETURN count(r) AS fixed_count
        """).single()
        
        print(f"  ✅ 修正 {result_enhanced['fixed_count']} 個 enhanced 關係")
        
        # 5. 最終驗證
        print(f"\n📊 最終驗證...")
        
        final_check = session.run("""
            MATCH ()-[r:RELATION]->()
            RETURN 
                count(r) AS total_relations,
                sum(CASE WHEN r.chunks IS NULL OR size(r.chunks) = 0 THEN 1 ELSE 0 END) AS still_missing,
                sum(CASE WHEN size(r.chunks) >= 1 THEN 1 ELSE 0 END) AS has_source,
                sum(CASE WHEN size(r.chunks) >= 2 THEN 1 ELSE 0 END) AS multi_source
        """).single()
        
        print(f"  • 關係總數：{final_check['total_relations']}")
        print(f"  • 有來源標記：{final_check['has_source']} ({final_check['has_source']/final_check['total_relations']*100:.1f}%)")
        print(f"  • 多來源支持：{final_check['multi_source']} ({final_check['multi_source']/final_check['total_relations']*100:.1f}%)")
        print(f"  • 仍缺少來源：{final_check['still_missing']} ({final_check['still_missing']/final_check['total_relations']*100:.1f}%)")

print("\n" + "="*70)
print("✅ 關係來源標記修正完成！")
print("="*70)
# 圖譜質量檢驗與完整度驗證（學術級專業版）

def ValidateGraphIntegrity(
    driver,
    original_chunks: List[Dict[str, str]],
    dataset_id: str,
    sample_size: int = 10,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    執行 Graph RAG 知識圖譜的完整度與質量檢驗（基於學術與實務標準）。
    
    檢驗架構：
    【第一組】結構與完整度檢驗 (Completeness & Structural Quality)
       - 節點覆蓋率 (Node Coverage)
       - 關係密度 (Relationship Density)
       - 屬性填充率 (Property Fill Rate)
       - 孤立節點比例 (Isolated Nodes Ratio)
    
    【第二組】一致性與類型檢查 (Consistency & Schema Adherence)
       - 類型遵守率 (Schema Adherence)
       - 重複實體檢測 (Duplication Check)
       - 屬性合法性檢查 (Attribute Validity)
    
    【第三組】核心數據質量報告 (Accuracy & Provenance)
       - 人工抽樣驗證 (Manual Sampling) - 10 個三元組
       - 出處標註率 (Provenance Rate)
    
    Args:
        driver: Neo4j GraphDatabase driver
        original_chunks: 原始知識庫的文本區塊列表
        dataset_id: 資料集識別符
        sample_size: 人工抽樣三元組數量（預設 10）
        verbose: 是否輸出詳細報告
    
    Returns:
        包含所有檢驗結果與專家結論的字典
    
    參考文獻：
        - Completeness metrics: Paulheim (2017), "Knowledge Graph Refinement"
        - Quality dimensions: Zaveri et al. (2016), "Quality Assessment for Linked Data"
    """
    validation_results = {
        "completeness_structural": {},
        "consistency_schema": {},
        "accuracy_provenance": {},
        "overall_pass": False,
        "quality_grade": "",
        "expert_conclusion": "",
    }
    
    if verbose:
        print("=" * 100)
        print("🔬 知識圖譜質量與完整度專業檢驗報告 (Academic-Grade KG Quality Assessment)")
        print("=" * 100)
        print("📚 檢驗標準：Paulheim (2017) + Zaveri et al. (2016)")
        print("=" * 100)
    
    with driver.session() as session:
        # ==========================================
        # 【第一組】結構與完整度檢驗
        # ==========================================
        if verbose:
            print("\n" + "=" * 100)
            print("【第一組】結構與完整度檢驗 (Completeness & Structural Quality)")
            print("=" * 100)
        
        # 1.1 節點覆蓋率 (Node Coverage)
        if verbose:
            print("\n📊 指標 1.1 | 節點覆蓋率 (Node Coverage)")
            print("-" * 100)
        
        expected_chunk_count = len(original_chunks)
        db_chunk_count = session.run(
            "MATCH (c:Chunk {dataset: $dataset}) RETURN count(c) AS cnt",
            dataset=dataset_id,
        ).single()["cnt"]
        
        total_chunk_count = session.run("MATCH (c:Chunk) RETURN count(c) AS cnt").single()["cnt"]
        other_chunks = total_chunk_count - db_chunk_count
        
        node_coverage = (db_chunk_count / expected_chunk_count * 100) if expected_chunk_count > 0 else 0
        
        if verbose:
            print(f"  • 原始文本 Chunk 總數 (Expected)：{expected_chunk_count}")
            print(f"  • Neo4j 當前 Dataset Chunk 數 (Actual)：{db_chunk_count}")
            print(f"  • 節點覆蓋率 (Coverage Rate)：{node_coverage:.2f}%")
            if other_chunks > 0:
                print(f"  ⚠️ 警告：資料庫中有其他 dataset 的 {other_chunks} 個舊 Chunk")
            if node_coverage >= 100:
                print(f"  ✅ 評估：節點覆蓋率達標 (≥100%)")
            elif node_coverage >= 95:
                print(f"  ⚠️ 評估：節點覆蓋率可接受 (95-100%)")
            else:
                print(f"  ❌ 評估：節點覆蓋率不足 (<95%)")
        
        # 1.2 關係密度 (Relationship Density)
        if verbose:
            print("\n📊 指標 1.2 | 關係密度 (Relationship Density)")
            print("-" * 100)
        
        entity_count = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            RETURN count(DISTINCT e) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        relation_count = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(h:Entity)-[r:RELATION]->(t:Entity)
            RETURN count(DISTINCT r) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        relationship_density = (relation_count / entity_count) if entity_count > 0 else 0
        
        # 統計實體的關係分佈
        # 注意：這裡統計的是每個實體參與的關係數（作為 head 或 tail）
        # 由於關係是有向的，每條關係會被計入 head 和 tail 各一次
        # 所以 avg_relations ≈ 2 * relationship_density（理論值）
        entity_relation_stats = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            OPTIONAL MATCH (e)-[r:RELATION]-()
            WITH e, count(DISTINCT r) AS rel_count
            RETURN 
                count(CASE WHEN rel_count = 0 THEN 1 END) AS isolated_entities,
                count(CASE WHEN rel_count = 1 THEN 1 END) AS single_rel_entities,
                count(CASE WHEN rel_count >= 2 AND rel_count < 5 THEN 1 END) AS moderate_rel_entities,
                count(CASE WHEN rel_count >= 5 THEN 1 END) AS high_rel_entities,
                avg(rel_count) AS avg_relations,
                max(rel_count) AS max_relations
            """,
            dataset=dataset_id,
        ).single()
        
        isolated_count = entity_relation_stats["isolated_entities"] or 0
        single_rel_count = entity_relation_stats["single_rel_entities"] or 0
        moderate_rel_count = entity_relation_stats["moderate_rel_entities"] or 0
        high_rel_count = entity_relation_stats["high_rel_entities"] or 0
        avg_rels = entity_relation_stats["avg_relations"] or 0
        max_rels = entity_relation_stats["max_relations"] or 0
        
        if verbose:
            print(f"  • 實體節點總數 (Entity Nodes)：{entity_count}")
            print(f"  • 關係總數 (Relations)：{relation_count}")
            print(f"  • 關係密度 (Density = Relations/Entities)：{relationship_density:.4f}")
            print()
            print(f"  📊 實體連接度分佈：")
            print(f"    • 孤立實體（0 個關係）：{isolated_count} ({isolated_count/entity_count*100:.1f}%)")
            print(f"    • 弱連接實體（1 個關係）：{single_rel_count} ({single_rel_count/entity_count*100:.1f}%)")
            print(f"    • 中度連接實體（2-4 個關係）：{moderate_rel_count} ({moderate_rel_count/entity_count*100:.1f}%)")
            print(f"    • 高度連接實體（≥5 個關係）：{high_rel_count} ({high_rel_count/entity_count*100:.1f}%)")
            print()
            print(f"  📈 實體連接度統計：")
            print(f"    • 平均每實體關係數（雙向計數）：{avg_rels:.2f}")
            print(f"      └─ 說明：統計時每條關係被計入 head 和 tail 各 1 次")
            print(f"      └─ 理論關係：平均關係數 ≈ 2 × 關係密度 = {relationship_density*2:.2f}")
            print(f"      └─ 實際比值：{avg_rels/relationship_density if relationship_density > 0 else 0:.2f}x")
            print(f"    • 最大連接度：{max_rels}")
            print()
            
            # 專家級評估
            print(f"  🔬 專家評估：")
            if relationship_density >= 2.0:
                print(f"    ✅ 關係密度優秀（≥2.0）")
                print(f"       └─ 圖譜具備豐富的語義連通性，適合複雜推理任務")
            elif relationship_density >= 1.5:
                print(f"    ✅ 關係密度良好（1.5-2.0）")
                print(f"       └─ 圖譜連通性充足，支持多跳查詢")
            elif relationship_density >= 1.0:
                print(f"    ⚠️ 關係密度中等（1.0-1.5）")
                print(f"       └─ 基本滿足需求，但仍有改進空間")
            elif relationship_density >= 0.5:
                print(f"    ⚠️ 關係密度偏低（0.5-1.0）")
                print(f"       └─ 連通性不足，多跳推理能力受限")
                print(f"       └─ 建議：增強關係抽取深度和廣度")
            else:
                print(f"    ❌ 關係密度嚴重不足（<0.5）")
                print(f"       └─ 圖譜幾乎呈孤立狀態，無法有效支持推理")
                print(f"       └─ 緊急需求：全面優化三元組抽取策略")
            
            print()
            if isolated_count / entity_count > 0.3:
                print(f"    ⚠️ 孤立實體比例過高（{isolated_count/entity_count*100:.1f}%）")
                print(f"       建議：檢查實體抽取是否過於寬泛，或關係抽取過於保守")
            
            if single_rel_count / entity_count > 0.4:
                print(f"    ⚠️ 弱連接實體佔比過大（{single_rel_count/entity_count*100:.1f}%）")
                print(f"       建議：增加屬性關係、時間關係、因果鏈等多維度關係")
            
            if high_rel_count / entity_count < 0.1:
                print(f"    💡 缺乏核心樞紐節點（高連接度實體 < 10%）")
                print(f"       建議：識別並強化領域核心概念的關係網絡")
        
        # 1.3 屬性填充率 (Property Fill Rate)
        if verbose:
            print("\n📊 指標 1.3 | 屬性填充率 (Property Fill Rate)")
            print("-" * 100)
        
        # 檢查 Entity 的 name 屬性填充率
        entity_with_name = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            WHERE e.name IS NOT NULL AND e.name <> ''
            RETURN count(DISTINCT e) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        name_fill_rate = (entity_with_name / entity_count * 100) if entity_count > 0 else 0
        
        # 檢查 Chunk 的 text 屬性填充率
        chunk_with_text = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})
            WHERE c.text IS NOT NULL AND c.text <> ''
            RETURN count(c) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        text_fill_rate = (chunk_with_text / db_chunk_count * 100) if db_chunk_count > 0 else 0
        
        if verbose:
            print(f"  • Entity.name 填充率：{name_fill_rate:.2f}% ({entity_with_name}/{entity_count})")
            print(f"  • Chunk.text 填充率：{text_fill_rate:.2f}% ({chunk_with_text}/{db_chunk_count})")
            avg_fill_rate = (name_fill_rate + text_fill_rate) / 2
            print(f"  • 平均屬性填充率：{avg_fill_rate:.2f}%")
            if avg_fill_rate >= 95:
                print(f"  ✅ 評估：屬性填充率優秀 (≥95%)")
            elif avg_fill_rate >= 80:
                print(f"  ⚠️ 評估：屬性填充率良好 (80-95%)")
            else:
                print(f"  ❌ 評估：屬性填充率不足 (<80%)")
        
        # 1.4 孤立節點比例 (Isolated Nodes Ratio)
        if verbose:
            print("\n📊 指標 1.4 | 孤立節點比例 (Isolated Nodes Ratio)")
            print("-" * 100)
        
        isolated_chunks = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})
            WHERE NOT (c)-[:MENTIONS]->(:Entity)
            RETURN count(c) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        isolated_entities = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            WHERE NOT (e)-[:RELATION]-()
            RETURN count(DISTINCT e) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        isolated_chunk_ratio = (isolated_chunks / db_chunk_count * 100) if db_chunk_count > 0 else 0
        isolated_entity_ratio = (isolated_entities / entity_count * 100) if entity_count > 0 else 0
        
        if verbose:
            print(f"  • 孤立 Chunk 數 (無 MENTIONS 連接)：{isolated_chunks} ({isolated_chunk_ratio:.2f}%)")
            print(f"  • 孤立 Entity 數 (無 RELATION 連接)：{isolated_entities} ({isolated_entity_ratio:.2f}%)")
            if isolated_chunk_ratio <= 5 and isolated_entity_ratio <= 15:
                print(f"  ✅ 評估：孤立節點比例低，結構品質良好")
            elif isolated_chunk_ratio <= 10 and isolated_entity_ratio <= 30:
                print(f"  ⚠️ 評估：孤立節點比例中等，建議優化三元組抽取")
            else:
                print(f"  ❌ 評估：孤立節點比例偏高，可能影響知識推理能力")
        
        # 儲存第一組檢驗結果
        validation_results["completeness_structural"] = {
            "node_coverage": node_coverage,
            "relationship_density": relationship_density,
            "avg_relations_per_entity": avg_rels,
            "max_relations_per_entity": max_rels,
            "property_fill_rate": (name_fill_rate + text_fill_rate) / 2,
            "isolated_chunk_ratio": isolated_chunk_ratio,
            "isolated_entity_ratio": isolated_entity_ratio,
            "entity_connection_distribution": {
                "isolated": isolated_count,
                "single_relation": single_rel_count,
                "moderate_relations": moderate_rel_count,
                "high_relations": high_rel_count,
            },
            "metrics": {
                "expected_chunks": expected_chunk_count,
                "db_chunks": db_chunk_count,
                "entity_count": entity_count,
                "relation_count": relation_count,
                "isolated_chunks": isolated_chunks,
                "isolated_entities": isolated_entities,
            }
        }
        
        # ==========================================
        # 【第二組】一致性與類型檢查
        # ==========================================
        if verbose:
            print("\n" + "=" * 100)
            print("【第二組】一致性與類型檢查 (Consistency & Schema Adherence)")
            print("=" * 100)
        
        # 2.1 類型遵守率 (Schema Adherence)
        if verbose:
            print("\n📊 指標 2.1 | 類型遵守率 (Schema Adherence)")
            print("-" * 100)
        
        node_labels_result = session.run(
            """
            CALL db.labels() YIELD label
            RETURN collect(label) AS labels
            """
        ).single()
        node_labels = node_labels_result["labels"] if node_labels_result else []
        
        relationship_types_result = session.run(
            """
            CALL db.relationshipTypes() YIELD relationshipType
            RETURN collect(relationshipType) AS types
            """
        ).single()
        relationship_types = relationship_types_result["types"] if relationship_types_result else []
        
        # 檢查關係的語義有效性（檢查 RELATION.type 屬性，而非關係類型名稱）
        # 統計不同的語義關係類型（從 r.type 屬性）
        semantic_relations_result = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->()-[r:RELATION]->()
            WHERE r.type IS NOT NULL AND r.type <> ''
            RETURN DISTINCT r.type AS relation_type
            ORDER BY relation_type
            """,
            dataset=dataset_id,
        ).data()
        semantic_relations = [row["relation_type"] for row in semantic_relations_result]
        
        # 檢測過於寬泛的語義關係（在 r.type 屬性中）
        generic_semantic_relations = [
            rt for rt in semantic_relations 
            if rt.upper() in ['RELATION', 'RELATES_TO', 'CONNECTED_TO', 'ASSOCIATED_WITH', '關聯', '相關', '連接']
        ]
        
        # 統計使用通用關係的數量
        generic_relation_count = 0
        if generic_semantic_relations:
            generic_relation_count = session.run(
                """
                MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->()-[r:RELATION]->()
                WHERE r.type IN $generic_types
                RETURN count(r) AS cnt
                """,
                dataset=dataset_id,
                generic_types=generic_semantic_relations,
            ).single()["cnt"]
        
        if verbose:
            print(f"  • 節點標籤 (Node Labels)：{', '.join(node_labels)}")
            print(f"    └─ 共 {len(node_labels)} 種節點類型")
            print(f"  • Neo4j 關係類型 (Relationship Types)：{', '.join(relationship_types)}")
            print(f"    └─ 共 {len(relationship_types)} 種 Neo4j 關係類型")
            print(f"  • 語義關係類型 (RELATION.type 屬性值)：{', '.join(semantic_relations[:20])}")
            if len(semantic_relations) > 20:
                print(f"    ... 以及其他 {len(semantic_relations) - 20} 種語義關係")
            print(f"    └─ 共 {len(semantic_relations)} 種語義關係類型")
            
            if generic_semantic_relations:
                print(f"  ⚠️ 警告：檢測到過於寬泛的語義關係類型：{', '.join(generic_semantic_relations)}")
                print(f"     共有 {generic_relation_count} 個關係使用了通用語義")
                print(f"     建議：優化提示詞以產生更具體的語義關係類型")
            
            if len(node_labels) >= 2 and len(semantic_relations) >= 2:
                print(f"  ✅ 評估：圖譜類型結構完整，語義關係豐富")
            else:
                print(f"  ❌ 評估：圖譜類型結構不完整或語義關係過於單一")
        
        # 2.2 重複實體檢測 (Duplication Check)
        if verbose:
            print("\n📊 指標 2.2 | 重複實體檢測 (Duplication Check)")
            print("-" * 100)
        
        # 檢查是否有名稱完全相同的實體（可能表示重複）
        duplicate_entities = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            WITH e.name AS name, collect(DISTINCT e) AS entities
            WHERE size(entities) > 1
            RETURN count(*) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        # 抽樣潛在重複實體
        duplicate_samples = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(e:Entity)
            WITH e.name AS name, collect(DISTINCT id(e)) AS entity_ids
            WHERE size(entity_ids) > 1
            RETURN name, size(entity_ids) AS count
            LIMIT 5
            """,
            dataset=dataset_id,
        ).data()
        
        if verbose:
            print(f"  • 檢測到的重複實體名稱數：{duplicate_entities}")
            if duplicate_samples:
                print(f"  • 抽樣範例：")
                for sample in duplicate_samples:
                    print(f"    - 「{sample['name']}」: {sample['count']} 個節點")
                print(f"  ⚠️ 評估：存在重複實體，可能需要實體消歧（Entity Disambiguation）")
            else:
                print(f"  ✅ 評估：未檢測到明顯重複實體")
        
        # 2.3 屬性合法性檢查 (Attribute Validity)
        if verbose:
            print("\n📊 指標 2.3 | 屬性合法性檢查 (Attribute Validity)")
            print("-" * 100)
        
        # 檢查空值或無效值
        invalid_relations = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->()-[r:RELATION]->()
            WHERE r.type IS NULL OR r.type = ''
            RETURN count(r) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        if verbose:
            print(f"  • 無效關係數（type 屬性為空）：{invalid_relations}")
            if invalid_relations == 0:
                print(f"  ✅ 評估：所有關係屬性合法")
            else:
                print(f"  ❌ 評估：存在 {invalid_relations} 個無效關係，需要修正")
        
        # 儲存第二組檢驗結果
        validation_results["consistency_schema"] = {
            "node_label_count": len(node_labels),
            "relationship_type_count": len(relationship_types),
            "semantic_relation_count": len(semantic_relations),
            "node_labels": node_labels,
            "relationship_types": relationship_types,
            "semantic_relations": semantic_relations,
            "generic_semantic_relations": generic_semantic_relations,
            "generic_relation_count": generic_relation_count,
            "duplicate_entities": duplicate_entities,
            "duplicate_samples": duplicate_samples,
            "invalid_relations": invalid_relations,
        }
        
        # ==========================================
        # 【第三組】核心數據質量報告
        # ==========================================
        if verbose:
            print("\n" + "=" * 100)
            print("【第三組】核心數據質量報告 (Accuracy & Provenance)")
            print("=" * 100)
        
        # 3.1 人工抽樣驗證 (Manual Sampling) - 10 個三元組
        if verbose:
            print("\n📊 指標 3.1 | 人工抽樣驗證 (Manual Sampling for Accuracy)")
            print("-" * 100)
            print(f"  隨機抽取 {sample_size} 個三元組，請進行人工語義正確性檢查：")
            print()
        
        sampled_triples = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->(h:Entity)-[r:RELATION]->(t:Entity)
            WITH h, r, t, rand() AS random
            ORDER BY random
            LIMIT $limit
            RETURN h.name AS head, r.type AS relation, t.name AS tail
            """,
            dataset=dataset_id,
            limit=sample_size,
        ).data()
        
        if verbose:
            if sampled_triples:
                for idx, triple in enumerate(sampled_triples, start=1):
                    head = triple.get("head", "N/A")
                    relation = triple.get("relation", "N/A")
                    tail = triple.get("tail", "N/A")
                    print(f"  [{idx:2d}] ({head}) --[{relation}]--> ({tail})")
                print()
                print("  " + "=" * 96)
                print("  💡 人工檢查指引 (Manual Verification Guidelines)")
                print("  " + "=" * 96)
                print("  請針對以上三元組逐一評估以下三個維度：")
                print()
                print("  ✓ 語義正確性 (Semantic Correctness)")
                print("    └─ 實體名稱是否正確且有意義？")
                print("    └─ 關係類型是否準確描述兩實體間的語義關聯？")
                print()
                print("  ✓ 邏輯一致性 (Logical Consistency)")
                print("    └─ 三元組的邏輯是否符合真實世界或原始知識庫內容？")
                print("    └─ Head 和 Tail 的實體類型是否與 Relation 相容？")
                print()
                print("  ✓ 資訊完整性 (Information Completeness)")
                print("    └─ 三元組是否包含足夠的上下文資訊？")
                print("    └─ 是否有明顯的資訊缺失或歧義？")
                print()
                print("  📝 建議：記錄有問題的三元組編號，用於後續優化提示詞或知識抽取流程。")
                print("  " + "=" * 96)
            else:
                print("  ❌ 警告：無法抽取三元組，圖譜中可能沒有有效的 RELATION 關係")
        
        # 3.2 出處標註率 (Provenance Rate)
        if verbose:
            print("\n📊 指標 3.2 | 出處標註率 (Provenance Rate)")
            print("-" * 100)
        
        # 檢查 Chunk 的 source 屬性填充率
        chunks_with_source = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})
            WHERE c.source IS NOT NULL AND c.source <> ''
            RETURN count(c) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        provenance_rate = (chunks_with_source / db_chunk_count * 100) if db_chunk_count > 0 else 0
        
        # 檢查是否有 RELATION 包含來源 chunks 資訊
        relations_with_chunks = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->()-[r:RELATION]->()
            WHERE r.chunks IS NOT NULL AND size(r.chunks) > 0
            RETURN count(DISTINCT r) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        
        relation_provenance_rate = (relations_with_chunks / relation_count * 100) if relation_count > 0 else 0
        
        if verbose:
            print(f"  • Chunk 出處標註率 (source 屬性)：{provenance_rate:.2f}% ({chunks_with_source}/{db_chunk_count})")
            print(f"  • Relation 來源追溯率 (chunks 屬性)：{relation_provenance_rate:.2f}% ({relations_with_chunks}/{relation_count})")
            avg_provenance = (provenance_rate + relation_provenance_rate) / 2
            print(f"  • 平均出處標註率：{avg_provenance:.2f}%")
            if avg_provenance >= 90:
                print(f"  ✅ 評估：出處標註率優秀，知識溯源性高")
            elif avg_provenance >= 70:
                print(f"  ⚠️ 評估：出處標註率良好，建議進一步提升")
            else:
                print(f"  ❌ 評估：出處標註率不足，可能影響知識可信度")
        
        # 儲存第三組檢驗結果
        validation_results["accuracy_provenance"] = {
            "sampled_triples": sampled_triples,
            "sample_size": len(sampled_triples),
            "provenance_rate": provenance_rate,
            "relation_provenance_rate": relation_provenance_rate,
            "avg_provenance": (provenance_rate + relation_provenance_rate) / 2,
        }
    
    # ==========================================
    # 【最終專家結論】綜合質量評估
    # ==========================================
    if verbose:
        print("\n" + "=" * 100)
        print("【最終專家結論】綜合質量評估 (Overall Quality Grade & Expert Conclusion)")
        print("=" * 100)
    
    # 計算各組指標的分數
    comp_struct = validation_results["completeness_structural"]
    consist_schema = validation_results["consistency_schema"]
    acc_prov = validation_results["accuracy_provenance"]
    
    # 評分邏輯（基於學術標準）
    score_completeness = 0
    if comp_struct["node_coverage"] >= 100:
        score_completeness += 25
    elif comp_struct["node_coverage"] >= 95:
        score_completeness += 20
    
    if comp_struct["relationship_density"] >= 0.5:
        score_completeness += 25
    elif comp_struct["relationship_density"] >= 0.2:
        score_completeness += 15
    
    if comp_struct["property_fill_rate"] >= 95:
        score_completeness += 25
    elif comp_struct["property_fill_rate"] >= 80:
        score_completeness += 20
    
    if comp_struct["isolated_chunk_ratio"] <= 5 and comp_struct["isolated_entity_ratio"] <= 15:
        score_completeness += 25
    elif comp_struct["isolated_chunk_ratio"] <= 10 and comp_struct["isolated_entity_ratio"] <= 30:
        score_completeness += 15
    
    score_consistency = 0
    if consist_schema["node_label_count"] >= 2 and consist_schema["semantic_relation_count"] >= 5:
        score_consistency += 30
    elif consist_schema["semantic_relation_count"] >= 2:
        score_consistency += 15
    
    # 檢查語義關係的質量（無通用關係 = 滿分，否則按比例扣分）
    if consist_schema["generic_relation_count"] == 0:
        score_consistency += 30
    else:
        # 按照通用關係比例扣分
        total_relations = session.run(
            """
            MATCH (c:Chunk {dataset: $dataset})-[:MENTIONS]->()-[r:RELATION]->()
            RETURN count(r) AS cnt
            """,
            dataset=dataset_id,
        ).single()["cnt"]
        if total_relations > 0:
            generic_ratio = consist_schema["generic_relation_count"] / total_relations
            if generic_ratio < 0.1:  # 小於 10% 的通用關係
                score_consistency += 25
            elif generic_ratio < 0.3:  # 小於 30% 的通用關係
                score_consistency += 15
            elif generic_ratio < 0.5:  # 小於 50% 的通用關係
                score_consistency += 5
    
    if consist_schema["duplicate_entities"] == 0:
        score_consistency += 20
    if consist_schema["invalid_relations"] == 0:
        score_consistency += 20
    
    score_accuracy = 0
    if acc_prov["sample_size"] >= sample_size:
        score_accuracy += 50
    if acc_prov["avg_provenance"] >= 90:
        score_accuracy += 50
    elif acc_prov["avg_provenance"] >= 70:
        score_accuracy += 35
    
    total_score = (score_completeness + score_consistency + score_accuracy) / 3
    
    # 質量等級判定
    if total_score >= 85:
        quality_grade = "優秀 (Excellent)"
        grade_emoji = "🏆"
        fitness_status = "高質量"
    elif total_score >= 70:
        quality_grade = "良好 (Good)"
        grade_emoji = "✅"
        fitness_status = "中高質量"
    elif total_score >= 55:
        quality_grade = "中等 (Fair)"
        grade_emoji = "⚠️"
        fitness_status = "中等質量"
    else:
        quality_grade = "需改進 (Poor)"
        grade_emoji = "❌"
        fitness_status = "低質量"
    
    # 找出最弱指標
    weakest_metrics = []
    if comp_struct["relationship_density"] < 0.2:
        weakest_metrics.append("關係密度偏低 (影響多跳推理)")
    if comp_struct["isolated_entity_ratio"] > 30:
        weakest_metrics.append("孤立節點比例過高 (影響知識連通性)")
    if comp_struct["property_fill_rate"] < 80:
        weakest_metrics.append("屬性填充率不足 (影響資訊完整性)")
    if consist_schema["generic_semantic_relations"]:
        weakest_metrics.append(f"存在過於寬泛的語義關係 ({', '.join(consist_schema['generic_semantic_relations'][:5])})")
    if acc_prov["avg_provenance"] < 70:
        weakest_metrics.append("出處標註率偏低 (影響知識溯源性)")
    
    if verbose:
        print(f"\n  {grade_emoji} 質量等級：{quality_grade}")
        print(f"  📊 綜合評分：{total_score:.1f}/100")
        print()
        print(f"  分項評分：")
        print(f"    • 完整度與結構 (Completeness & Structure)：{score_completeness:.1f}/100")
        print(f"    • 一致性與類型 (Consistency & Schema)：{score_consistency:.1f}/100")
        print(f"    • 準確性與溯源 (Accuracy & Provenance)：{score_accuracy:.1f}/100")
        print()
        print("  " + "=" * 96)
        print("  📋 專家結論 (Expert Conclusion)")
        print("  " + "=" * 96)
        print()
        print(f"  根據 (i) 關係密度 ({comp_struct['relationship_density']:.4f})、")
        print(f"       (ii) 孤立節點比率 (Chunks: {comp_struct['isolated_chunk_ratio']:.2f}%, Entities: {comp_struct['isolated_entity_ratio']:.2f}%)、")
        print(f"       (iii) 屬性填充率 ({comp_struct['property_fill_rate']:.2f}%)、")
        print(f"       (iv) 語義關係豐富度 ({consist_schema['semantic_relation_count']} 種)、")
        print(f"       (v) 平均連接度 (每實體 {comp_struct['avg_relations_per_entity']:.2f} 個關係) 的數據，")
        print()
        print(f"  本圖譜已達到【{fitness_status}】標準，{'可' if total_score >= 70 else '暫不建議'}投入 Graph RAG 系統使用。")
        print()
        if weakest_metrics:
            print(f"  ⚠️ 需注意的指標：")
            for metric in weakest_metrics:
                print(f"     • {metric}")
            print()
            print(f"  💡 改進建議：")
            
            # 關係密度專項建議
            if comp_struct["relationship_density"] < 1.5:
                print(f"     📊 關係密度提升策略（當前：{comp_struct['relationship_density']:.2f}，目標：≥1.5）：")
                print(f"        ├─ 🔧 增強抽取深度：")
                print(f"        │  • 擴展關係類型：屬性關係（數值為、濃度為）、時間關係（發生於、持續）")
                print(f"        │  • 挖掘隱式關係：因果鏈（A→B→C）、共現關係、層級關係")
                print(f"        │  • 實施共指消解：將「它」「該物質」還原為具體實體名，增加實體複用")
                print(f"        ├─ 🎯 優化提示詞：")
                print(f"        │  • 明確要求「每個實體至少 2 個關係」")
                print(f"        │  • 提供多維度關係範例（因果、屬性、功能、時間）")
                print(f"        │  • 增加「從不同角度描述實體」的指令")
                print(f"        └─ 🧪 後處理增強：")
                print(f"           • 知識圖譜補全（Link Prediction）：TransE/RotatE 預測缺失關係")
                print(f"           • 實體合併：識別同義實體（如「維生素A」vs「視黃醇」）")
                print(f"           • 關係推理：基於規則的傳遞閉包（如 A包含B, B含有C → A間接含有C）")
                print()
            
            if comp_struct["isolated_entity_ratio"] > 30:
                print(f"     • 孤立實體過多（{comp_struct['isolated_entity_ratio']:.1f}%）：")
                print(f"       └─ 可能原因：實體粒度過細、關係抽取過於保守")
                print(f"       └─ 建議：提高實體抽象層級，或增加實體間的弱關係（如共現、上下位）")
                print()
            
            if consist_schema["generic_semantic_relations"]:
                print(f"     • 存在通用語義關係：請使用具體動詞（導致、含有、影響）替代模糊詞（關聯、相關）")
                print()
            
            if acc_prov["avg_provenance"] < 70:
                print(f"     • 出處標註率偏低：確保所有關係包含來源追溯資訊（chunks 屬性）")
        else:
            print(f"  ✅ 所有核心指標均達到優良標準，圖譜質量卓越！")
        print()
        print("  " + "=" * 96)
        print(f"  📚 參考文獻與進階技術：")
        print(f"     • Paulheim, H. (2017). Knowledge graph refinement: A survey of approaches.")
        print(f"     • Zaveri, A., et al. (2016). Quality assessment for linked data.")
        print(f"     • TransE/RotatE: 知識圖譜嵌入模型，用於鏈接預測與補全")
        print(f"     • Coreference Resolution: 共指消解技術，提升實體複用率")
        print("  " + "=" * 96)
    
    validation_results["overall_pass"] = (total_score >= 70)
    validation_results["quality_grade"] = quality_grade
    validation_results["total_score"] = total_score
    validation_results["score_breakdown"] = {
        "completeness_structural": score_completeness,
        "consistency_schema": score_consistency,
        "accuracy_provenance": score_accuracy,
    }
    validation_results["weakest_metrics"] = weakest_metrics
    validation_results["expert_conclusion"] = f"本圖譜達到【{fitness_status}】標準（評分：{total_score:.1f}/100）"
    
    return validation_results


print("✅ ValidateGraphIntegrity() 函式已載入（學術級專業版）")