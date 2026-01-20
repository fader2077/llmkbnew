"""
快速验证加速版优化器是否正常工作
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from neo4j import GraphDatabase
from ollama import Client
from config import CONFIG
from src.optimizer import GraphOptimizer

def quick_test():
    """快速测试加速版函数是否可用"""
    
    print("="*70)
    print("🧪 快速验证加速版 GraphOptimizer")
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
    
    # 连接 Ollama
    try:
        ollama_client = Client(host=CONFIG["infrastructure"]["ollama_host"])
        print("✅ Ollama 连接成功")
    except Exception as e:
        print(f"❌ Ollama 连接失败: {e}")
        driver.close()
        return
    
    # 创建优化器
    try:
        optimizer = GraphOptimizer(
            driver=driver,
            client=ollama_client,
            model=CONFIG["models"]["llm_model"],
            max_workers=2  # 测试用较小值
        )
        print(f"✅ GraphOptimizer 创建成功 (workers={optimizer.max_workers})")
    except Exception as e:
        print(f"❌ GraphOptimizer 创建失败: {e}")
        driver.close()
        return
    
    # 测试函数签名
    print("\n" + "="*70)
    print("📋 检查函数签名")
    print("="*70)
    
    if hasattr(optimizer, 'infer_weak_links_accelerated'):
        print("✅ infer_weak_links_accelerated 方法存在")
        
        # 检查函数注解
        func = optimizer.infer_weak_links_accelerated
        annotations = func.__annotations__
        print(f"   参数注解: {annotations}")
        
        if 'return' in annotations:
            print(f"   ✅ 返回类型已定义: {annotations['return']}")
        else:
            print("   ⚠️  返回类型未定义")
    else:
        print("❌ infer_weak_links_accelerated 方法不存在")
    
    # 检查数据库状态
    print("\n" + "="*70)
    print("📊 数据库状态检查")
    print("="*70)
    
    with driver.session() as session:
        # 检查实体数量
        entity_count = session.run("MATCH (e:Entity) RETURN count(e) as cnt").single()["cnt"]
        print(f"   实体总数: {entity_count}")
        
        # 检查关系数量
        relation_count = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) as cnt").single()["cnt"]
        print(f"   关系总数: {relation_count}")
        
        # 检查弱实体数量
        weak_count = session.run("""
            MATCH (e:Entity)
            WHERE size((e)--()) < 2
            RETURN count(e) as cnt
        """).single()["cnt"]
        print(f"   弱实体数量 (度<2): {weak_count}")
        
        if entity_count == 0:
            print("\n⚠️  数据库为空，请先运行 Phase 1 构建图谱")
        elif weak_count == 0:
            print("\n✅ 没有弱实体需要处理")
        else:
            print(f"\n💡 可以优化 {weak_count} 个弱实体")
    
    print("\n" + "="*70)
    print("✅ 验证完成！")
    print("="*70)
    print("\n💡 提示：")
    print("   1. 运行 main.py 并选择 Phase 3b")
    print("   2. 选择策略 2（弱连接实体全局关系推理）")
    print("   3. 观察「🚀 加速版」标记")
    print("   4. 查看处理速度和新增关系数")
    
    driver.close()

if __name__ == "__main__":
    quick_test()
