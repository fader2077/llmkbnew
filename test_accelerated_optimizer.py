"""
测试加速版 GraphOptimizer 的性能提升

对比：
1. 标准版（逐个实体处理）
2. 加速版（批次处理 + 并行执行）
"""

import time
from neo4j import GraphDatabase
from ollama import Client
from config import CONFIG

def test_accelerated_optimizer():
    """测试加速版优化器"""
    
    # 连接数据库
    driver = GraphDatabase.driver(
        CONFIG["infrastructure"]["neo4j_uri"],
        auth=CONFIG["infrastructure"]["neo4j_auth"]
    )
    
    # 连接 Ollama
    ollama_client = Client(host=CONFIG["infrastructure"]["ollama_host"])
    
    # 导入优化器
    from src.optimizer import GraphOptimizer
    from src.inspector import GraphInspector
    
    print("\n" + "="*70)
    print("🚀 测试加速版 GraphOptimizer")
    print("="*70)
    
    # 创建优化器（使用 4 个 workers）
    optimizer = GraphOptimizer(
        driver=driver,
        client=ollama_client,
        model=CONFIG["models"]["llm_model"],
        max_workers=2
    )
    
    # 创建诊断器
    inspector = GraphInspector(driver)
    
    # 优化前状态
    print("\n📊 优化前状态...")
    before_stats = inspector.run_basic_diagnosis(verbose=False)
    print(f"   节点数: {before_stats['entity_count']}")
    print(f"   关系数: {before_stats['relation_count']}")
    print(f"   密度: {before_stats['density']:.2f}")
    print(f"   弱连接实体: {before_stats['weak_entities_count']} ({before_stats['weak_entities_percent']:.1f}%)")
    
    # 执行加速版弱连接推理
    print("\n" + "="*70)
    start_time = time.time()
    optimizer.infer_weak_links_accelerated(degree_threshold=2)
    elapsed = time.time() - start_time
    print("="*70)
    print(f"⏱️  执行时间: {elapsed:.2f} 秒")
    
    # 优化后状态
    print("\n📊 优化后状态...")
    after_stats = inspector.run_basic_diagnosis(verbose=False)
    print(f"   节点数: {after_stats['entity_count']}")
    print(f"   关系数: {after_stats['relation_count']} (+{after_stats['relation_count'] - before_stats['relation_count']})")
    print(f"   密度: {after_stats['density']:.2f} (+{after_stats['density'] - before_stats['density']:.2f})")
    print(f"   弱连接实体: {after_stats['weak_entities_count']} ({after_stats['weak_entities_percent']:.1f}%)")
    
    # 计算改进
    improvement = {
        'new_relations': after_stats['relation_count'] - before_stats['relation_count'],
        'density_increase': after_stats['density'] - before_stats['density'],
        'weak_entities_reduced': before_stats['weak_entities_count'] - after_stats['weak_entities_count'],
        'execution_time': elapsed
    }
    
    print("\n" + "="*70)
    print("📈 改进总结")
    print("="*70)
    print(f"✅ 新增关系: {improvement['new_relations']}")
    print(f"✅ 密度提升: {improvement['density_increase']:.2f}")
    print(f"✅ 弱实体减少: {improvement['weak_entities_reduced']}")
    print(f"⏱️  总耗时: {improvement['execution_time']:.2f} 秒")
    print(f"⚡ 平均速度: {improvement['new_relations'] / improvement['execution_time']:.1f} 关系/秒")
    
    driver.close()

if __name__ == "__main__":
    test_accelerated_optimizer()
