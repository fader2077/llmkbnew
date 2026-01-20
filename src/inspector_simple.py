# src/inspector.py (简化版)
"""
Structure diagnosis  (Phase 3)
图谱结构诊断和验证
"""

from typing import Dict, Any


class GraphInspector:
    """图谱诊断工具"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def run_basic_diagnosis(self) -> Dict[str, Any]:
        """执行基础诊断"""
        print("="*70)
        print("🔍 图谱基础诊断")
        print("="*70)
        
        with self.driver.session() as session:
            # 计算基本统计
            total_chunks = session.run("MATCH (c:Chunk) RETURN count(c) AS cnt").single()["cnt"]
            total_entities = session.run("MATCH (e:Entity) RETURN count(e) AS cnt").single()["cnt"]
            total_relations = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt").single()["cnt"]
            total_mentions = session.run("MATCH ()-[m:MENTIONS]->() RETURN count(m) AS cnt").single()["cnt"]
            
            # 孤立节点
            isolated_entities = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)-[:RELATION]-()
                RETURN count(e) AS cnt
            """).single()["cnt"]
            
            # 平均度数
            avg_degree = session.run("""
                MATCH (e:Entity)
                OPTIONAL MATCH (e)-[r:RELATION]-()
                WITH e, count(r) AS degree
                RETURN avg(degree) AS avg_degree
            """).single()["avg_degree"] or 0.0
            
        density = total_relations / total_entities if total_entities > 0 else 0.0
        
        print(f"\n📊 统计结果：")
        print(f"  • Chunks: {total_chunks:,}")
        print(f"  • Entities: {total_entities:,}")
        print(f"  • Relations: {total_relations:,}")
        print(f"  • Mentions: {total_mentions:,}")
        print(f"  • 孤立实体: {isolated_entities:,} ({isolated_entities/total_entities*100:.1f}%)" if total_entities > 0 else "  • 孤立实体: 0")
        print(f"  • 关系密度: {density:.3f}")
        print(f"  • 平均度数: {avg_degree:.2f}")
        print("="*70)
        
        return {
            "chunks": total_chunks,
            "entities": total_entities,
            "relations": total_relations,
            "mentions": total_mentions,
            "isolated_entities": isolated_entities,
            "density": density,
            "avg_degree": avg_degree
        }
