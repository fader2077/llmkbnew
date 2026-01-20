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
            # 注意：對於 RAG 系統，我們關注的是「有效密度」(E/V)，而非學術定義的 E/(V*(V-1))
            # 學術密度對大圖會趨近於 0，不適合作為優化目標
            academic_density = (relation_type_count / (total_entities * (total_entities - 1))) if total_entities > 1 else 0
            effective_density = (relation_type_count / total_entities) if total_entities > 0 else 0  # 即 avg_degree / 2
            avg_degree = (2 * relation_type_count / total_entities) if total_entities > 0 else 0
            
            results = {
                "chunks": total_chunks,
                "entities": total_entities,
                "relations_total": relation_type_count + mentions_count,
                "mentions_count": mentions_count,
                "relation_count": relation_type_count,
                "density": effective_density,  # 使用有效密度代替學術密度
                "academic_density": academic_density,  # 保留學術密度供參考
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
                print(f"  • 有效密度（E/V）：{effective_density:.3f}  👈 RAG 優化目標")
                print(f"  • 平均度數（2E/V）：{avg_degree:.2f}")
                print(f"  • 學術密度（參考）：{academic_density:.6f}")
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

    def run_comprehensive_quality_check(self, dataset_id: str, verbose: bool = True) -> Dict[str, Any]:
        """
        執行完整的學術級圖譜質量檢驗
        
        檢驗維度：
        1. 結構完整度 (Completeness)
        2. 連接質量 (Connectivity Quality)
        3. 實體度數分布 (Degree Distribution)
        4. 關係類型多樣性 (Relation Type Diversity)
        5. 潛在質量問題 (Quality Issues)
        
        參考標準：
        - Paulheim (2017), "Knowledge Graph Refinement"
        - Zaveri et al. (2016), "Quality Assessment for Linked Data"
        """
        results = {
            "basic_metrics": {},
            "connectivity_quality": {},
            "degree_distribution": {},
            "relation_diversity": {},
            "quality_issues": {},
            "overall_grade": ""
        }
        
        if verbose:
            print("\n" + "="*100)
            print("🔬 知識圖譜質量與完整度學術級檢驗報告")
            print("="*100)
            print("📚 檢驗標準：Paulheim (2017) + Zaveri et al. (2016)")
            print("="*100)
        
        with self.driver.session() as session:
            # ═══════════════════════════════════════════════════════════════
            # 第一部分：基礎指標
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print("\n📊 一、基礎結構指標")
                print("-"*100)
            
            entity_count = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
            relation_count = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
            chunk_count = session.run("MATCH (c:Chunk {dataset: $dataset}) RETURN count(c) AS cnt", dataset=dataset_id).single()["cnt"]
            mentions_count = session.run("MATCH ()-[m:MENTIONS]->() RETURN count(m) AS cnt").single()["cnt"]
            
            # 計算關係密度（每個實體平均有多少關係）
            density = (relation_count / entity_count) if entity_count > 0 else 0.0
            
            # 計算平均度數（雙向計數）
            avg_degree_result = session.run("""
                MATCH (e:Entity)
                OPTIONAL MATCH (e)-[r:RELATION]-()
                WITH e, count(r) AS degree
                RETURN avg(degree) AS avg_degree
            """).single()
            avg_degree = avg_degree_result["avg_degree"] if avg_degree_result else 0.0
            
            results["basic_metrics"] = {
                "entities": entity_count,
                "relations": relation_count,
                "chunks": chunk_count,
                "mentions": mentions_count,
                "density": density,
                "avg_degree": avg_degree
            }
            
            if verbose:
                print(f"  • 實體節點數：{entity_count:,}")
                print(f"  • 語義關係數：{relation_count:,}")
                print(f"  • 文本 Chunks：{chunk_count:,}")
                print(f"  • MENTIONS 連接：{mentions_count:,}")
                print(f"  • 關係密度 (R/E)：{density:.3f}")
                print(f"  • 平均度數：{avg_degree:.2f}")
            
            # ═══════════════════════════════════════════════════════════════
            # 第二部分：連接質量分析
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print(f"\n🔗 二、連接質量分析")
                print("-"*100)
            
            # 1. 孤立實體（度數 = 0）
            isolated_entities = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)-[:RELATION]-()
                RETURN count(e) AS cnt
            """).single()["cnt"]
            isolated_percent = (isolated_entities / entity_count * 100) if entity_count > 0 else 0
            
            # 2. 弱連接實體（度數 1-3）
            weak_entities = session.run("""
                MATCH (e:Entity)-[r:RELATION]-()
                WITH e, count(r) AS degree
                WHERE degree >= 1 AND degree <= 3
                RETURN count(e) AS cnt
            """).single()["cnt"]
            weak_percent = (weak_entities / entity_count * 100) if entity_count > 0 else 0
            
            # 3. 中度連接實體（度數 4-9）
            moderate_entities = session.run("""
                MATCH (e:Entity)-[r:RELATION]-()
                WITH e, count(r) AS degree
                WHERE degree >= 4 AND degree <= 9
                RETURN count(e) AS cnt
            """).single()["cnt"]
            moderate_percent = (moderate_entities / entity_count * 100) if entity_count > 0 else 0
            
            # 4. 強連接實體（度數 >= 10）
            strong_entities = session.run("""
                MATCH (e:Entity)-[r:RELATION]-()
                WITH e, count(r) AS degree
                WHERE degree >= 10
                RETURN count(e) AS cnt
            """).single()["cnt"]
            strong_percent = (strong_entities / entity_count * 100) if entity_count > 0 else 0
            
            results["connectivity_quality"] = {
                "isolated": {"count": isolated_entities, "percent": isolated_percent},
                "weak": {"count": weak_entities, "percent": weak_percent},
                "moderate": {"count": moderate_entities, "percent": moderate_percent},
                "strong": {"count": strong_entities, "percent": strong_percent}
            }
            
            if verbose:
                print(f"  1. 孤立實體（度數=0）：{isolated_entities:,} ({isolated_percent:.1f}%)")
                print(f"     {'✅ 優秀' if isolated_percent < 5 else '⚠️ 需注意' if isolated_percent < 15 else '❌ 需改進'}")
                print(f"  2. 弱連接實體（度數1-3）：{weak_entities:,} ({weak_percent:.1f}%)")
                print(f"     {'✅ 優秀' if weak_percent < 30 else '⚠️ 需注意' if weak_percent < 50 else '❌ 需改進'}")
                print(f"  3. 中度連接實體（度數4-9）：{moderate_entities:,} ({moderate_percent:.1f}%)")
                print(f"  4. 強連接實體（度數≥10）：{strong_entities:,} ({strong_percent:.1f}%)")
                print(f"     {'✅ 優秀' if strong_percent >= 10 else '⚠️ 待優化' if strong_percent >= 5 else '❌ 需改進'}")
            
            # ═══════════════════════════════════════════════════════════════
            # 第三部分：實體度數分布統計
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print(f"\n📈 三、實體度數分布")
                print("-"*100)
            
            degree_distribution = session.run("""
                MATCH (e:Entity)
                OPTIONAL MATCH (e)-[r:RELATION]-()
                WITH e, count(r) AS degree
                RETURN degree, count(e) AS entity_count
                ORDER BY degree DESC
                LIMIT 20
            """).data()
            
            results["degree_distribution"] = degree_distribution
            
            if verbose:
                print(f"  度數分布（前 20）：")
                for dist in degree_distribution[:10]:
                    print(f"    度數 {dist['degree']:3d}：{dist['entity_count']:,} 個實體")
            
            # ═══════════════════════════════════════════════════════════════
            # 第四部分：關係類型多樣性
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print(f"\n🎨 四、關係類型多樣性")
                print("-"*100)
            
            relation_type_count = session.run("""
                MATCH ()-[r:RELATION]->()
                RETURN count(DISTINCT r.type) AS cnt
            """).single()["cnt"]
            
            relation_types = session.run("""
                MATCH ()-[r:RELATION]->()
                RETURN r.type AS relation_type, count(r) AS cnt
                ORDER BY cnt DESC
                LIMIT 10
            """).data()
            
            results["relation_diversity"] = {
                "total_types": relation_type_count,
                "top_types": relation_types
            }
            
            if verbose:
                print(f"  • 關係類型總數：{relation_type_count}")
                print(f"  • 前 10 種關係類型：")
                for idx, rel in enumerate(relation_types, 1):
                    percent = (rel['cnt'] / relation_count * 100) if relation_count > 0 else 0
                    print(f"    {idx:2d}. {rel['relation_type']:<40s} {rel['cnt']:>6,} ({percent:>5.1f}%)")
            
            # ═══════════════════════════════════════════════════════════════
            # 第五部分：潛在質量問題檢測
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print(f"\n⚠️  五、潛在質量問題檢測")
                print("-"*100)
            
            issues_found = []
            
            # 檢測 1：自環關係
            self_loops = session.run("""
                MATCH (e:Entity)-[r:RELATION]->(e)
                RETURN count(r) AS cnt
            """).single()["cnt"]
            if self_loops > 0:
                issues_found.append(f"發現 {self_loops} 個自環關係")
            
            # 檢測 2：重複關係
            duplicate_relations = session.run("""
                MATCH (h:Entity)-[r:RELATION]->(t:Entity)
                WITH h, t, r.type AS rel_type, count(r) AS cnt
                WHERE cnt > 1
                RETURN count(*) AS dup_cnt
            """).single()["dup_cnt"]
            if duplicate_relations > 0:
                issues_found.append(f"發現 {duplicate_relations} 組重複關係")
            
            # 檢測 3：超長實體名稱
            long_entities = session.run("""
                MATCH (e:Entity)
                WHERE size(e.name) > 50
                RETURN count(e) AS cnt
            """).single()["cnt"]
            if long_entities > 0:
                issues_found.append(f"發現 {long_entities} 個超長實體名稱（>50字元）")
            
            # 檢測 4：空屬性
            empty_chunks_relations = session.run("""
                MATCH ()-[r:RELATION]->()
                WHERE r.chunks IS NULL OR r.chunks = []
                RETURN count(r) AS cnt
            """).single()["cnt"]
            if empty_chunks_relations > 0:
                issues_found.append(f"發現 {empty_chunks_relations} 個關係缺少來源標記")
            
            results["quality_issues"] = {
                "self_loops": self_loops,
                "duplicate_relations": duplicate_relations,
                "long_entities": long_entities,
                "empty_chunks_relations": empty_chunks_relations,
                "issues_list": issues_found
            }
            
            if verbose:
                if issues_found:
                    for issue in issues_found:
                        print(f"  ⚠️  {issue}")
                else:
                    print("  ✅ 未發現明顯質量問題")
            
            # ═══════════════════════════════════════════════════════════════
            # 最終評級
            # ═══════════════════════════════════════════════════════════════
            if verbose:
                print(f"\n{'='*100}")
                print(f"🏆 最終質量評級")
                print(f"{'='*100}")
            
            score = 0
            max_score = 7
            
            # 評分維度
            if density >= 2.0:
                score += 1
                density_status = "✅"
            elif density >= 1.5:
                score += 0.5
                density_status = "⚠️"
            else:
                density_status = "❌"
            
            if avg_degree >= 4.0:
                score += 1
                degree_status = "✅"
            elif avg_degree >= 2.5:
                score += 0.5
                degree_status = "⚠️"
            else:
                degree_status = "❌"
            
            if isolated_percent < 5:
                score += 1
                isolated_status = "✅"
            elif isolated_percent < 15:
                score += 0.5
                isolated_status = "⚠️"
            else:
                isolated_status = "❌"
            
            if weak_percent < 30:
                score += 1
                weak_status = "✅"
            elif weak_percent < 50:
                score += 0.5
                weak_status = "⚠️"
            else:
                weak_status = "❌"
            
            if strong_percent >= 10:
                score += 1
                strong_status = "✅"
            elif strong_percent >= 5:
                score += 0.5
                strong_status = "⚠️"
            else:
                strong_status = "❌"
            
            if relation_type_count >= 50:
                score += 1
                diversity_status = "✅"
            elif relation_type_count >= 30:
                score += 0.5
                diversity_status = "⚠️"
            else:
                diversity_status = "❌"
            
            if len(issues_found) == 0:
                score += 1
                quality_status = "✅"
            elif len(issues_found) <= 2:
                score += 0.5
                quality_status = "⚠️"
            else:
                quality_status = "❌"
            
            if verbose:
                print(f"  {density_status} 關係密度 ≥ 2.0：{density:.3f}")
                print(f"  {degree_status} 平均度數 ≥ 4.0：{avg_degree:.2f}")
                print(f"  {isolated_status} 孤立實體 < 5%：{isolated_percent:.1f}%")
                print(f"  {weak_status} 弱連接實體 < 30%：{weak_percent:.1f}%")
                print(f"  {strong_status} 強連接實體 ≥ 10%：{strong_percent:.1f}%")
                print(f"  {diversity_status} 關係類型 ≥ 50：{relation_type_count}")
                print(f"  {quality_status} 無質量問題：{'是' if len(issues_found) == 0 else '否'}")
                print(f"\n  總分：{score:.1f}/{max_score}")
            
            if score >= 6.5:
                grade = "A+ 卓越"
            elif score >= 5.5:
                grade = "A 優秀"
            elif score >= 4.5:
                grade = "B 良好"
            elif score >= 3.5:
                grade = "C 及格"
            else:
                grade = "D 待改進"
            
            results["overall_grade"] = grade
            results["score"] = score
            results["max_score"] = max_score
            
            if verbose:
                print(f"  等級：{grade}")
                print(f"{'='*100}\n")
        
        return results

    def check_quality_issues(self) -> Dict[str, int]:
        """
        檢查圖譜質量問題，返回統計數據
        
        Returns:
            Dict 包含:
                - self_loops: 自環關係數量
                - duplicate_relations: 重複關係組數
                - empty_chunks: 缺失來源標記的關係數量
                - isolated_entities: 孤立實體數量
                - weak_entities: 弱連接實體數量（度數1-3）
        """
        results = {}
        
        with self.driver.session() as session:
            # 1. 檢查自環關係
            self_loops = session.run("""
                MATCH (e:Entity)-[r:RELATION]->(e)
                RETURN count(r) AS cnt
            """).single()["cnt"]
            
            # 2. 檢查重複關係
            duplicate_relations = session.run("""
                MATCH (h:Entity)-[r:RELATION]->(t:Entity)
                WITH h, r.type AS rel_type, t, collect(r) AS rels
                WHERE size(rels) > 1
                RETURN count(*) AS cnt
            """).single()["cnt"]
            
            # 3. 檢查缺失來源標記的關係
            empty_chunks = session.run("""
                MATCH ()-[r:RELATION]->()
                WHERE r.chunks IS NULL OR r.chunks = [] OR size(r.chunks) = 0
                RETURN count(r) AS cnt
            """).single()["cnt"]
            
            # 4. 檢查孤立實體
            isolated_entities = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)-[:RELATION]-()
                RETURN count(e) AS cnt
            """).single()["cnt"]
            
            # 5. 檢查弱連接實體（度數1-3）
            weak_entities = session.run("""
                MATCH (e:Entity)
                OPTIONAL MATCH (e)-[r:RELATION]-()
                WITH e, count(r) AS degree
                WHERE degree >= 1 AND degree <= 3
                RETURN count(e) AS cnt
            """).single()["cnt"]
            
            results = {
                "self_loops": self_loops,
                "duplicate_relations": duplicate_relations,
                "empty_chunks": empty_chunks,
                "isolated_entities": isolated_entities,
                "weak_entities": weak_entities
            }
        
        return results
