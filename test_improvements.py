# test_improvements.py
"""
测试 Phase 1 数据库清理和 Phase 3b 质量修复功能
"""
from neo4j import GraphDatabase
from config import CONFIG

def test_quality_fixes():
    """测试质量问题修复功能"""
    print("\n" + "="*70)
    print("🧪 测试质量问题修复功能")
    print("="*70)
    
    driver = GraphDatabase.driver(
        CONFIG["infrastructure"]["neo4j_uri"],
        auth=CONFIG["infrastructure"]["neo4j_auth"]
    )
    
    try:
        # 1. 检查修复前的状态
        print("\n📊 检查修复前的质量问题...")
        from src.inspector import GraphInspector
        inspector = GraphInspector(driver)
        before_issues = inspector.check_quality_issues()
        
        print(f"  • 自环关系：{before_issues['self_loops']}")
        print(f"  • 重复关系：{before_issues['duplicate_relations']}")
        print(f"  • 缺失来源：{before_issues['empty_chunks']}")
        print(f"  • 孤立实体：{before_issues['isolated_entities']}")
        print(f"  • 弱连接实体：{before_issues['weak_entities']}")
        
        # 2. 执行质量修复
        if (before_issues['self_loops'] > 0 or 
            before_issues['duplicate_relations'] > 0 or 
            before_issues['empty_chunks'] > 0):
            
            print("\n🔧 执行质量修复...")
            from src.optimizer import GraphOptimizer
            from ollama import Client as OllamaClient
            
            ollama_client = OllamaClient(host=CONFIG["infrastructure"]["ollama_host"])
            optimizer = GraphOptimizer(
                driver=driver,
                client=ollama_client,
                model=CONFIG["models"]["llm_model"]
            )
            
            fix_results = optimizer.fix_quality_issues()
            print(f"  • 移除自环：{fix_results['self_loops_removed']}")
            print(f"  • 合并重复：{fix_results['duplicate_relations_merged']}")
            print(f"  • 修复缺失：{fix_results['empty_chunks_fixed']}")
            
            # 3. 检查修复后的状态
            print("\n📊 检查修复后的质量问题...")
            after_issues = inspector.check_quality_issues()
            
            print(f"  • 自环关系：{after_issues['self_loops']} (修复前: {before_issues['self_loops']})")
            print(f"  • 重复关系：{after_issues['duplicate_relations']} (修复前: {before_issues['duplicate_relations']})")
            print(f"  • 缺失来源：{after_issues['empty_chunks']} (修复前: {before_issues['empty_chunks']})")
            
            if (after_issues['self_loops'] == 0 and 
                after_issues['duplicate_relations'] == 0 and 
                after_issues['empty_chunks'] < before_issues['empty_chunks']):
                print("\n✅ 质量修复成功！")
            else:
                print("\n⚠️  部分问题可能未完全修复")
        else:
            print("\n✅ 没有发现质量问题，无需修复")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

def test_database_cleanup():
    """测试数据库清理功能"""
    print("\n" + "="*70)
    print("🧪 测试数据库清理功能")
    print("="*70)
    
    driver = GraphDatabase.driver(
        CONFIG["infrastructure"]["neo4j_uri"],
        auth=CONFIG["infrastructure"]["neo4j_auth"]
    )
    
    try:
        from src.database import clean_database
        from src.inspector import GraphInspector
        
        inspector = GraphInspector(driver)
        
        # 检查清理前的状态
        print("\n📊 清理前的数据库状态...")
        before_stats = inspector.run_basic_diagnosis(verbose=False)
        print(f"  • 实体数：{before_stats['entities']}")
        print(f"  • 关系数：{before_stats['relation_count']}")
        print(f"  • Chunks：{before_stats['chunks']}")
        
        # 执行清理
        print("\n🗑️  执行数据库清理...")
        clean_database(driver, "", clean_all=True)
        
        # 检查清理后的状态
        print("\n📊 清理后的数据库状态...")
        after_stats = inspector.run_basic_diagnosis(verbose=False)
        print(f"  • 实体数：{after_stats['entities']}")
        print(f"  • 关系数：{after_stats['relation_count']}")
        print(f"  • Chunks：{after_stats['chunks']}")
        
        if (after_stats['entities'] == 0 and 
            after_stats['relation_count'] == 0 and 
            after_stats['chunks'] == 0):
            print("\n✅ 数据库清理成功！")
        else:
            print("\n⚠️  数据库可能未完全清理")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 开始测试改进功能")
    print("="*70)
    
    # 测试1：质量修复功能
    test_quality_fixes()
    
    # 测试2：数据库清理功能（谨慎使用！会清空数据库）
    response = input("\n⚠️  是否测试数据库清理功能？这将清空当前数据库！(yes/no): ")
    if response.strip().lower() == "yes":
        test_database_cleanup()
    else:
        print("跳过数据库清理测试")
    
    print("\n" + "="*70)
    print("✅ 测试完成")
    print("="*70)
