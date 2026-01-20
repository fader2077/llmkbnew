# src/database.py
"""
Neo4j 資料庫連接與管理模組

提供：
- Neo4jConnector 類別：封裝 Neo4j 連接
- clean_database：資料清理函數
- ensure_vector_index：向量索引建立
- ensure_fulltext_index：全文索引建立
"""

from typing import Dict
from neo4j import GraphDatabase


class Neo4jConnector:
    """Neo4j 資料庫連接器"""
    
    def __init__(self, uri: str, auth: tuple):
        """
        初始化連接
        
        Args:
            uri: Neo4j 連接 URI (例如: "bolt://localhost:7687")
            auth: 認證元組 (username, password)
        """
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.uri = uri
    
    def close(self):
        """關閉連接"""
        if self.driver:
            self.driver.close()
    
    def get_driver(self):
        """獲取底層 driver 對象"""
        return self.driver
    
    def verify_connectivity(self):
        """驗證連接是否正常"""
        self.driver.verify_connectivity()
        print(f"✅ Neo4j 連接成功: {self.uri}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def clean_database(driver, dataset_id: str, clean_all: bool = False) -> Dict[str, int]:
    """
    清理 Neo4j 資料庫中的舊資料。
    
    Args:
        driver: Neo4j GraphDatabase driver
        dataset_id: 要清理的資料集 ID
        clean_all: 若為 True，清理所有資料；否則僅清理指定 dataset_id 的資料
    
    Returns:
        刪除的節點和關係統計
    """
    with driver.session() as session:
        if clean_all:
            print("🗑️ 清理所有資料...")
            # 刪除所有節點和關係
            deleted_relations = session.run("MATCH ()-[r]->() DELETE r RETURN count(r) AS cnt").single()["cnt"]
            deleted_nodes = session.run("MATCH (n) DELETE n RETURN count(n) AS cnt").single()["cnt"]
            print(f"  ✅ 已刪除 {deleted_nodes} 個節點, {deleted_relations} 個關係")
            
            return {
                "deleted_chunks": deleted_nodes,
                "deleted_mentions": deleted_relations,
                "deleted_entities": 0,
                "deleted_relations": 0,
            }
        else:
            print(f"🗑️ 清理 dataset_id = '{dataset_id}' 的資料...")
            
            # 刪除與指定 dataset 相關的 Chunk 節點及其關係
            deleted_mentions = session.run(
                """
                MATCH (c:Chunk {dataset: $dataset})-[m:MENTIONS]->(:Entity)
                DELETE m
                RETURN count(m) AS cnt
                """,
                dataset=dataset_id,
            ).single()["cnt"]
            
            # 清理孤立的 Entity 和 RELATION
            deleted_relations = session.run(
                """
                MATCH (e:Entity)
                WHERE NOT (e)<-[:MENTIONS]-(:Chunk)
                MATCH (e)-[r:RELATION]-()
                DELETE r
                RETURN count(r) AS cnt
                """
            ).single()["cnt"]
            
            deleted_entities = session.run(
                """
                MATCH (e:Entity)
                WHERE NOT (e)<-[:MENTIONS]-(:Chunk)
                  AND NOT (e)-[:RELATION]-()
                DELETE e
                RETURN count(e) AS cnt
                """
            ).single()["cnt"]
            
            deleted_chunks = session.run(
                """
                MATCH (c:Chunk {dataset: $dataset})
                DELETE c
                RETURN count(c) AS cnt
                """,
                dataset=dataset_id,
            ).single()["cnt"]
            
            print(f"  ✅ 已刪除 {deleted_chunks} 個 Chunks")
            print(f"  ✅ 已刪除 {deleted_mentions} 個 MENTIONS 關係")
            print(f"  ✅ 已刪除 {deleted_entities} 個孤立 Entities")
            print(f"  ✅ 已刪除 {deleted_relations} 個孤立 RELATIONS")
            
            return {
                "deleted_chunks": deleted_chunks,
                "deleted_mentions": deleted_mentions,
                "deleted_entities": deleted_entities,
                "deleted_relations": deleted_relations,
            }


def ensure_entity_index(driver) -> None:
    """
    為 Entity 節點的 name 屬性創建索引（關鍵性能優化）
    
    這能讓 MERGE (e:Entity {name: $name}) 操作提升 10 倍以上的速度。
    
    Args:
        driver: Neo4j driver
    """
    with driver.session() as session:
        try:
            # 創建 Entity name 的唯一約束（自動包含索引）
            session.run(
                "CREATE CONSTRAINT entity_name_unique IF NOT EXISTS "
                "FOR (e:Entity) REQUIRE e.name IS UNIQUE"
            )
            print("  ✅ Entity name 唯一約束已創建（含索引）")
        except Exception as e:
            # 如果唯一約束已存在或失敗，嘗試創建普通索引
            try:
                session.run(
                    "CREATE INDEX entity_name_index IF NOT EXISTS "
                    "FOR (e:Entity) ON (e.name)"
                )
                print("  ✅ Entity name 索引已創建")
            except Exception as e2:
                print(f"  ⚠️  Entity 索引創建警告: {e2}")


def ensure_vector_index(
    driver, 
    name: str, 
    label: str, 
    prop: str, 
    dimensions: int, 
    similarity: str = "cosine"
) -> None:
    """
    確保向量索引存在
    
    Args:
        driver: Neo4j driver
        name: 索引名稱
        label: 節點標籤
        prop: 屬性名稱
        dimensions: 向量維度
        similarity: 相似度函數 ("cosine", "euclidean")
    """
    with driver.session() as session:
        # 檢查索引是否已存在
        existing = session.run("SHOW INDEXES").data()
        if any(idx.get("name") == name for idx in existing):
            print(f"  ✅ 向量索引 '{name}' 已存在")
            return
        
        # 創建向量索引
        cypher = f"""
        CREATE VECTOR INDEX {name}
        FOR (n:{label}) ON (n.{prop})
        OPTIONS {{ indexConfig: {{ `vector.dimensions`: {dimensions}, `vector.similarity_function`: '{similarity}' }} }}
        """
        session.run(cypher)
        session.run("CALL db.awaitIndexes()")
        print(f"  ✅ 已創建向量索引 '{name}' (維度={dimensions}, 相似度={similarity})")


def ensure_fulltext_index(driver, name: str, label: str, prop: str = "text") -> bool:
    """
    確保全文索引存在
    
    Args:
        driver: Neo4j driver
        name: 索引名稱
        label: 節點標籤
        prop: 屬性名稱
    
    Returns:
        索引是否可用
    """
    with driver.session() as session:
        # 檢查索引是否已存在
        existing = session.run("SHOW INDEXES").data()
        if any(idx.get("name") == name for idx in existing):
            print(f"  ✅ 全文索引 '{name}' 已存在")
            return True
        
        # 創建全文索引
        try:
            session.run(
                f"CREATE FULLTEXT INDEX {name} FOR (n:{label}) ON EACH [n.{prop}]"
            )
            session.run("CALL db.awaitIndexes()")
            print(f"  ✅ 已創建全文索引 '{name}'")
            return True
        except Exception as e:
            print(f"  ⚠️ 全文索引創建失敗: {e}")
            return False
