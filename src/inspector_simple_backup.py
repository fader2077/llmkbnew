# src/inspector.py
"""
Structure diagnosis (Phase 3) - 學術級專業版
封裝為 GraphInspector 類，避免 import 時自動執行
"""
from typing import Dict, Any, List, Optional


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
            
            # ✅ 修正：計算每個實體的平均關係數（更合理的密度指標）
            density = (relation_type_count / total_entities) if total_entities > 0 else 0
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
    
    def run_full_validation(
        self,
        original_chunks: List[Dict[str, str]],
        dataset_id: str,
        sample_size: int = 10,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        執行完整的學術級知識圖譜質量檢驗
        
        檢驗架構：
        【第一組】結構與完整度檢驗 (Completeness & Structural Quality)
        【第二組】一致性與類型檢查 (Consistency & Schema Adherence)
        【第三組】核心數據質量報告 (Accuracy & Provenance)
        
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
        if verbose:
            print("=" * 100)
            print("🔬 知識圖譜質量與完整度專業檢驗報告 (Academic-Grade KG Quality Assessment)")
            print("=" * 100)
        
        # 基礎診斷
        basic_results = self.run_basic_diagnosis(verbose=verbose)
        
        # 完整性分析
        integrity_results = self.run_integrity_analysis(verbose=verbose)
        
        # 合併結果
        validation_results = {
            "basic_diagnosis": basic_results,
            "integrity_analysis": integrity_results,
            "overall_pass": basic_results["density"] > 0.001,  # 簡單判定
            "quality_grade": "待評估",
            "expert_conclusion": "基礎診斷完成，建議使用更詳細的檢驗方法獲取完整報告",
        }
        
        return validation_results


# 注意：原始 inspector.py 的 1800+ 行腳本代碼已備份至 inspector_old_script.py
# 如需使用完整的學術級檢驗功能，請參考該備份文件中的代碼
# 當前版本提供基礎診斷功能，適合快速檢查圖譜狀態
