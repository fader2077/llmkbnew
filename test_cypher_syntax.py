"""
测试 Neo4j 5.x Cypher 语法修复

验证 COUNT { pattern } 语法是否正常工作
"""

from neo4j import GraphDatabase
from config import CONFIG

def test_cypher_syntax():
    """测试新的 Cypher 语法"""
    
    print("="*70)
    print("🧪 测试 Neo4j 5.x Cypher 语法修复")
    print("="*70)
    
    # 连接数据库
    try:
        driver = GraphDatabase.driver(
            CONFIG["infrastructure"]["neo4j_uri"],
            auth=CONFIG["infrastructure"]["neo4j_auth"]
        )
        print("✅ Neo4j 连接成功")
    except Exception as e:
        print(f"❌ Neo4j 连接失败: {e}")
        return
    
    with driver.session() as session:
        # 测试 1: 旧语法（应该失败）
        print("\n" + "="*70)
        print("❌ 测试 1: 旧语法 size((e)--())")
        print("="*70)
        try:
            result = session.run("""
                MATCH (e:Entity)
                WHERE size((e)--()) < 2
                RETURN count(e) as cnt
            """)
            count = result.single()["cnt"]
            print(f"⚠️  旧语法居然通过了？返回: {count}")
        except Exception as e:
            print(f"✅ 预期的错误: {str(e)[:200]}")
        
        # 测试 2: 新语法（应该成功）
        print("\n" + "="*70)
        print("✅ 测试 2: 新语法 COUNT { (e)--() }")
        print("="*70)
        try:
            result = session.run("""
                MATCH (e:Entity)
                WHERE COUNT { (e)--() } < 2
                RETURN count(e) as cnt
            """)
            count = result.single()["cnt"]
            print(f"✅ 新语法成功！找到 {count} 个弱实体（度 < 2）")
        except Exception as e:
            print(f"❌ 新语法失败: {e}")
        
        # 测试 3: 完整查询（optimizer.py 中使用的）
        print("\n" + "="*70)
        print("✅ 测试 3: 完整优化器查询")
        print("="*70)
        try:
            result = session.run("""
                MATCH (e:Entity)
                WHERE COUNT { (e)--() } < $threshold
                MATCH (e)<-[:MENTIONS]-(c:Chunk)
                WITH c, collect(DISTINCT e.name) AS weak_entities
                WHERE size(weak_entities) > 0
                RETURN c.id AS chunk_id, count(weak_entities) as entity_count
                LIMIT 5
            """, threshold=2)
            
            chunks = list(result)
            print(f"✅ 查询成功！找到 {len(chunks)} 个包含弱实体的 Chunks")
            for i, record in enumerate(chunks, 1):
                print(f"   {i}. Chunk ID: {record['chunk_id']}, 弱实体数: {record['entity_count']}")
        except Exception as e:
            print(f"❌ 完整查询失败: {e}")
        
        # 测试 4: 验证 WHERE NOT (e)--() 仍然有效（用于孤立节点）
        print("\n" + "="*70)
        print("✅ 测试 4: 孤立节点查询 WHERE NOT (e)--()")
        print("="*70)
        try:
            result = session.run("""
                MATCH (e:Entity)
                WHERE NOT (e)--()
                RETURN count(e) as cnt
            """)
            count = result.single()["cnt"]
            print(f"✅ 孤立节点查询成功！找到 {count} 个完全孤立的实体")
        except Exception as e:
            print(f"❌ 孤立节点查询失败: {e}")
    
    driver.close()
    
    print("\n" + "="*70)
    print("✅ 所有语法测试完成！")
    print("="*70)
    print("\n💡 现在可以运行 main.py -> Phase 3b -> 策略 2")

if __name__ == "__main__":
    test_cypher_syntax()
