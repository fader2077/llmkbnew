"""
数据救援脚本：修正 Optimizer 写入的关系格式

问题：optimizer.py 之前直接用语义（如 :CAUSES）作为关系类型
解决：转换为标准格式 :RELATION {type: 'CAUSES'}，与 builder.py 保持一致

使用方法：
    python rescue_relations.py

预期结果：
    - 找到所有非标准格式的关系（:CAUSES, :AFFECTS 等）
    - 转换为标准格式 :RELATION {type: 'xxx'}
    - 删除旧格式，避免重复
"""

from neo4j import GraphDatabase
from config import CONFIG
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rescue_relations():
    """数据救援：转换错误格式的关系"""
    
    # 连接数据库
    uri = CONFIG["neo4j"]["uri"]
    user = CONFIG["neo4j"]["user"]
    password = CONFIG["neo4j"]["password"]
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    print("\n" + "="*70)
    print("🚑 开始数据救援：修正关系格式")
    print("="*70)
    
    try:
        with driver.session() as session:
            # 步骤 1：统计需要修正的关系
            print("\n📊 步骤 1：统计需要修正的关系...")
            
            result = session.run("""
                MATCH (h:Entity)-[r]->(t:Entity)
                WHERE type(r) <> 'RELATION' AND type(r) <> 'MENTIONS'
                RETURN type(r) AS rel_type, count(r) AS count
                ORDER BY count DESC
            """)
            
            wrong_format_relations = list(result)
            
            if not wrong_format_relations:
                print("  ✅ 未发现需要修正的关系，图谱格式正确！")
                return
            
            print(f"  发现 {len(wrong_format_relations)} 种错误格式的关系类型：")
            total_wrong = 0
            for record in wrong_format_relations:
                count = record['count']
                total_wrong += count
                print(f"    - :{record['rel_type']}: {count:,} 条")
            
            print(f"\n  📌 总计需要修正：{total_wrong:,} 条关系")
            
            # 步骤 2：执行转换
            print("\n🔄 步骤 2：执行格式转换...")
            print("  策略：创建标准格式关系 → 删除旧格式关系")
            
            # 使用批处理以避免内存溢出
            batch_size = 1000
            converted_count = 0
            
            for rel_record in wrong_format_relations:
                rel_type = rel_record['rel_type']
                count = rel_record['count']
                
                print(f"\n  处理 :{rel_type} ({count:,} 条)...")
                
                # 分批处理
                offset = 0
                while offset < count:
                    # 构建查询（避免 f-string 类型检查问题）
                    query = """
                        MATCH (h:Entity)-[r]->(t:Entity)
                        WHERE type(r) = $rel_type
                        WITH h, t, r
                        LIMIT $batch_size
                        
                        // 创建标准格式关系
                        MERGE (h)-[new_r:RELATION {type: $rel_type}]->(t)
                        ON CREATE SET 
                            new_r.source = COALESCE(r.source, 'ai_inference'),
                            new_r.confidence = COALESCE(r.confidence, 0.8),
                            new_r.created_at = COALESCE(r.created_at, timestamp())
                        
                        // 删除旧格式关系
                        DELETE r
                        
                        RETURN count(r) AS converted
                    """
                    
                    result = session.run(query, batch_size=batch_size, rel_type=rel_type)
                    
                    record = result.single()
                    batch_converted = record['converted'] if record else 0
                    converted_count += batch_converted
                    offset += batch_size
                    
                    print(f"    进度：{min(offset, count):,} / {count:,}")
            
            # 步骤 3：验证结果
            print("\n✅ 步骤 3：验证转换结果...")
            
            # 检查是否还有错误格式
            remaining_result = session.run("""
                MATCH (h:Entity)-[r]->(t:Entity)
                WHERE type(r) <> 'RELATION' AND type(r) <> 'MENTIONS'
                RETURN count(r) AS cnt
            """).single()
            remaining = remaining_result['cnt'] if remaining_result else 0
            
            # 统计标准格式关系
            standard_result = session.run("""
                MATCH ()-[r:RELATION]->()
                RETURN count(r) AS cnt
            """).single()
            standard = standard_result['cnt'] if standard_result else 0
            
            print(f"\n{'='*70}")
            print(f"✅ 数据救援完成！")
            print(f"{'='*70}")
            print(f"  • 转换关系数：{converted_count:,} 条")
            print(f"  • 剩余错误格式：{remaining:,} 条")
            print(f"  • 当前标准格式关系总数：{standard:,} 条")
            
            if remaining == 0:
                print(f"\n  🎉 所有关系已成功转换为标准格式！")
            else:
                print(f"\n  ⚠️  仍有 {remaining} 条关系未转换（可能是特殊类型）")
            
            print(f"{'='*70}")
            
    except Exception as e:
        print(f"\n❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    
    finally:
        driver.close()

if __name__ == "__main__":
    print("\n⚠️  警告：此操作会修改数据库中的关系格式")
    print("建议在执行前备份数据库")
    
    confirm = input("\n是否继续？(y/n): ").strip().lower()
    
    if confirm == 'y':
        rescue_relations()
    else:
        print("已取消操作")
