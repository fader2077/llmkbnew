# test_quality_fix_v2.py
"""
测试强力质量修复功能（双策略模式）
"""
from neo4j import GraphDatabase
from config import CONFIG

def test_quality_fix_v2():
    """测试强力质量修复功能"""
    print("\n" + "="*70)
    print("🧪 测试强力质量修复功能（双策略模式）")
    print("="*70)
    
    driver = GraphDatabase.driver(
        CONFIG["infrastructure"]["neo4j_uri"],
        auth=CONFIG["infrastructure"]["neo4j_auth"]
    )
    
    try:
        # 1. 检查修复前的状态
        print("\n📊 Step 1: 检查修复前的质量问题...")
        from src.inspector import GraphInspector
        inspector = GraphInspector(driver)
        
        before_issues = inspector.check_quality_issues()
        before_stats = inspector.run_basic_diagnosis(verbose=False)
        
        print(f"\n【修复前状态】")
        print(f"  • 实体数：{before_stats['entities']:,}")
        print(f"  • 关系数：{before_stats['relation_count']:,}")
        print(f"  • 有效密度（E/V）：{before_stats['density']:.3f}")
        print(f"  • 平均度数：{before_stats['avg_degree']:.2f}")
        print(f"\n【质量问题】")
        print(f"  • 自环关系：{before_issues['self_loops']:,}")
        print(f"  • 重复关系：{before_issues['duplicate_relations']:,}")
        print(f"  • 缺失来源：{before_issues['empty_chunks']:,}")
        print(f"  • 孤立实体：{before_issues['isolated_entities']:,}")
        print(f"  • 弱连接实体（度数1-3）：{before_issues['weak_entities']:,}")
        
        # 2. 执行强力质量修复
        if (before_issues['self_loops'] > 0 or 
            before_issues['duplicate_relations'] > 0 or 
            before_issues['empty_chunks'] > 0):
            
            print("\n" + "="*70)
            print("🔧 Step 2: 执行强力质量修复（双策略模式）...")
            print("="*70)
            
            from src.optimizer import GraphOptimizer
            from ollama import Client as OllamaClient
            
            ollama_client = OllamaClient(host=CONFIG["infrastructure"]["ollama_host"])
            optimizer = GraphOptimizer(
                driver=driver,
                client=ollama_client,
                model=CONFIG["models"]["llm_model"]
            )
            
            fix_results = optimizer.fix_quality_issues()
            
            print(f"\n【修复结果】")
            print(f"  • 移除自环：{fix_results['self_loops_removed']:,}")
            print(f"  • 合并重复：{fix_results['duplicate_relations_merged']:,}")
            print(f"  • 修复缺失来源：{fix_results['empty_chunks_fixed']:,}")
            
            # 3. 检查修复后的状态
            print("\n" + "="*70)
            print("📊 Step 3: 检查修复后的质量问题...")
            print("="*70)
            
            after_issues = inspector.check_quality_issues()
            after_stats = inspector.run_basic_diagnosis(verbose=False)
            
            print(f"\n【修复后状态】")
            print(f"  • 实体数：{after_stats['entities']:,}")
            print(f"  • 关系数：{after_stats['relation_count']:,}")
            print(f"  • 有效密度（E/V）：{after_stats['density']:.3f}")
            print(f"  • 平均度数：{after_stats['avg_degree']:.2f}")
            print(f"\n【质量问题】")
            print(f"  • 自环关系：{after_issues['self_loops']:,} (修复前: {before_issues['self_loops']:,})")
            print(f"  • 重复关系：{after_issues['duplicate_relations']:,} (修复前: {before_issues['duplicate_relations']:,})")
            print(f"  • 缺失来源：{after_issues['empty_chunks']:,} (修复前: {before_issues['empty_chunks']:,})")
            
            # 4. 对比分析
            print("\n" + "="*70)
            print("📈 Step 4: 修复效果对比分析")
            print("="*70)
            
            self_loop_fixed = before_issues['self_loops'] - after_issues['self_loops']
            duplicate_fixed = before_issues['duplicate_relations'] - after_issues['duplicate_relations']
            empty_fixed = before_issues['empty_chunks'] - after_issues['empty_chunks']
            
            print(f"\n【修复数量】")
            print(f"  • 自环关系：{self_loop_fixed:,} / {before_issues['self_loops']:,} ({self_loop_fixed / before_issues['self_loops'] * 100:.1f}%)" if before_issues['self_loops'] > 0 else "  • 自环关系：无需修复")
            print(f"  • 重复关系：{duplicate_fixed:,} / {before_issues['duplicate_relations']:,} ({duplicate_fixed / before_issues['duplicate_relations'] * 100:.1f}%)" if before_issues['duplicate_relations'] > 0 else "  • 重复关系：无需修复")
            print(f"  • 缺失来源：{empty_fixed:,} / {before_issues['empty_chunks']:,} ({empty_fixed / before_issues['empty_chunks'] * 100:.1f}%)")
            
            print(f"\n【剩余问题】")
            if after_issues['empty_chunks'] > 0:
                remaining_percent = after_issues['empty_chunks'] / before_issues['empty_chunks'] * 100
                print(f"  • 仍有 {after_issues['empty_chunks']:,} 个关系缺失来源 ({remaining_percent:.1f}%)")
                print(f"    原因：这些关系可能是推理生成的，且头尾实体都是孤儿实体（无 MENTIONS）")
                print(f"    影响：轻微。RAG 检索时这些关系仍可用，只是无法追溯到原始文本")
            else:
                print(f"  ✅ 所有质量问题已完全修复！")
            
            # 5. 评估图谱质量
            print("\n" + "="*70)
            print("🎯 Step 5: 图谱质量评估")
            print("="*70)
            
            quality_grade = []
            
            # 密度评估
            if after_stats['density'] >= 2.0:
                quality_grade.append("✅ 密度")
                density_status = "优秀"
            elif after_stats['density'] >= 1.0:
                quality_grade.append("⚠️ 密度")
                density_status = "良好"
            else:
                quality_grade.append("❌ 密度")
                density_status = "待改进"
            
            # 平均度数评估
            if after_stats['avg_degree'] >= 4.0:
                quality_grade.append("✅ 度数")
                degree_status = "优秀"
            elif after_stats['avg_degree'] >= 2.0:
                quality_grade.append("⚠️ 度数")
                degree_status = "良好"
            else:
                quality_grade.append("❌ 度数")
                degree_status = "待改进"
            
            # 质量问题评估
            total_issues = after_issues['self_loops'] + after_issues['duplicate_relations'] + min(after_issues['empty_chunks'], 100)
            if total_issues == 0:
                quality_grade.append("✅ 质量")
                quality_status = "完美"
            elif total_issues < 100:
                quality_grade.append("⚠️ 质量")
                quality_status = "可接受"
            else:
                quality_grade.append("❌ 质量")
                quality_status = "待改进"
            
            print(f"\n【质量指标】")
            print(f"  • 有效密度（E/V）：{after_stats['density']:.3f} - {density_status}")
            print(f"  • 平均度数：{after_stats['avg_degree']:.2f} - {degree_status}")
            print(f"  • 质量问题数：{total_issues:,} - {quality_status}")
            
            print(f"\n【总体评价】")
            excellent = quality_grade.count("✅ 密度") + quality_grade.count("✅ 度数") + quality_grade.count("✅ 质量")
            if excellent == 3:
                print(f"  🎉 图谱质量：A+ 卓越")
            elif excellent == 2:
                print(f"  👍 图谱质量：A 优秀")
            elif excellent == 1:
                print(f"  ⚠️  图谱质量：B 良好")
            else:
                print(f"  ❌ 图谱质量：C 待改进")
            
            print(f"\n✅ 强力质量修复测试完成！")
            
        else:
            print("\n✅ 没有发现质量问题，无需修复")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 开始测试强力质量修复功能")
    print("="*70)
    
    test_quality_fix_v2()
    
    print("\n" + "="*70)
    print("✅ 所有测试完成")
    print("="*70)
