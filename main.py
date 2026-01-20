# main.py
"""
Graph RAG Pipeline - 统一执行入口

完整流程：
Phase 1: 索引消融测试
Phase 2: 图谱构建
Phase 3: 图谱优化与诊断  
Phase 4: 检索消融测试
"""

import sys
from pathlib import Path
from ollama import Client

# 确保可以 import src
sys.path.insert(0, str(Path(__file__).parent))

from config import CONFIG, KNOWLEDGE_BASE_PATH, QUESTION_DATASET_PATH
from src.database import Neo4jConnector, clean_database
from src.models import OllamaVectorEmbedder
from src.builder import GraphBuilder
from src.inspector import GraphInspector
#from src.optimizer import EnhanceGraphConnectivity
from src.retrieval import RetrievalEngine, test_retrieval
from src.experiments import RetrievalAblationRunner, IndexingAblationRunner


def print_menu():
    """显示主菜单"""
    print("\n" + "="*70)
    print("🚀 Graph RAG Pipeline - 主菜单")
    print("="*70)
    print()
    print("📚 阶段选择:")
    print("  1. 🏗️  Phase 1: 索引消融实验 (Indexing Ablation)")
    print("  2. 🏗️  Phase 2: 构建知识图谱 (Build Graph)")
    print("  3. 🔍 Phase 3a: 完整圖譜診斷 (Comprehensive Diagnosis)")
    print("  4. ⚡ Phase 3b: 圖譜擴增優化 (Graph Augmentation Optimization)")
    print("  5. 🧪 Phase 4: 检索消融实验 (Retrieval Ablation)")
    print("  6. 🎯 快速测试检索 (Quick Test Retrieval)")
    print("  9. 🗑️  清理数据库 (Clean Database)")
    print("  0. ❌ 退出 (Exit)")
    print()
    print("="*70)


def main():
    """主程序入口"""
    print("🔌 初始化连接...")
    
    # 1. 连接数据库
    try:
        db = Neo4jConnector(
            CONFIG["infrastructure"]["neo4j_uri"],
            CONFIG["infrastructure"]["neo4j_auth"]
        )
        db.verify_connectivity()
        driver = db.get_driver()
    except Exception as e:
        print(f"❌ Neo4j 连接失败: {e}")
        print(f"请确保 Neo4j 正在运行: {CONFIG['infrastructure']['neo4j_uri']}")
        return
    
    # 2. 连接 Ollama
    try:
        ollama_client = Client(host=CONFIG["infrastructure"]["ollama_host"])
        # 简单测试连接
        ollama_client.list()
        print(f"✅ Ollama 连接成功: {CONFIG['infrastructure']['ollama_host']}")
    except Exception as e:
        print(f"❌ Ollama 连接失败: {e}")
        print(f"请确保 Ollama 正在运行: {CONFIG['infrastructure']['ollama_host']}")
        db.close()
        return
    
    # 主循环
    try:
        while True:
            print_menu()
            choice = input("请选择操作 (0-6, 9): ").strip()
            
            if choice == "0":
                print("\n👋 再见！")
                break
            
            elif choice == "1":
                # Phase 1: 索引消融实验
                print("\n" + "="*70)
                print("🏗️  Phase 1: 索引消融实验")
                print("="*70)
                print("⚠️  此操作会自动清空并重建数据库，测试不同的 Chunk Size 配置")
                print()
                
                confirm = input("确认继续? (yes/no): ").strip().lower()
                if confirm != "yes":
                    print("❌ 已取消")
                    continue
                
                # 讀取 config.py 中的索引消融網格（indexing_grid）
                # 若 CONFIG 中不存在，使用安全的回退值
                chunk_configs = CONFIG.get("indexing_grid", [
                    {"chunk_size": 2048, "overlap": 512},
                ])
                
                try:
                    indexing_runner = IndexingAblationRunner(driver, ollama_client)
                    indexing_runner.run_experiment(
                        text_path=KNOWLEDGE_BASE_PATH,
                        chunk_configs=chunk_configs,
                        questions_path=QUESTION_DATASET_PATH,
                        max_questions=150
                    )
                    print("\n✅ Phase 1 完成！")
                except Exception as e:
                    print(f"\n❌ Phase 1 失败: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "2":
                # Phase 2: 构建图谱
                print("\n" + "="*70)
                print("🏗️  开始构建知识图谱...")
                print("="*70)
                
                if not KNOWLEDGE_BASE_PATH.exists():
                    print(f"❌ 知识库文件不存在: {KNOWLEDGE_BASE_PATH}")
                    continue
                
                try:
                    builder = GraphBuilder(driver, ollama_client)
                    builder.build_graph(KNOWLEDGE_BASE_PATH)
                    print("\n✅ 图谱构建完成！")
                except Exception as e:
                    print(f"\n❌ 构建失败: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "3":
                # Phase 3a: 完整診斷
                print("\n" + "="*70)
                print("🔍 Phase 3a: 完整圖譜診斷 (學術級)")
                print("="*70)
                print("檢驗維度：結構完整度、連接質量、度數分布、關係多樣性、質量問題")
                print()
                
                try:
                    inspector = GraphInspector(driver)
                    
                    # 執行完整的學術級診斷
                    results = inspector.run_comprehensive_quality_check(
                        dataset_id=CONFIG["infrastructure"]["dataset_id"],
                        verbose=True
                    )
                    
                    print("\n✅ 診斷完成！")
                    print(f"\n📋 診斷摘要：")
                    print(f"  • 實體總數：{results['basic_metrics']['entities']:,}")
                    print(f"  • 關係總數：{results['basic_metrics']['relations']:,}")
                    print(f"  • 關係密度：{results['basic_metrics']['density']:.3f}")
                    print(f"  • 平均度數：{results['basic_metrics']['avg_degree']:.2f}")
                    print(f"  • 孤立實體：{results['connectivity_quality']['isolated']['count']:,} ({results['connectivity_quality']['isolated']['percent']:.1f}%)")
                    print(f"  • 弱連接實體（度數1-3）：{results['connectivity_quality']['weak']['count']:,} ({results['connectivity_quality']['weak']['percent']:.1f}%)")
                    print(f"  • 質量評級：{results['overall_grade']}")
                    
                except Exception as e:
                    print(f"\n❌ 診斷失敗: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "4":
                # Phase 3b: 弱實體擴增優化
                print("\n" + "="*70)
                print("⚡ Phase 3b: 弱實體擴增優化")
                print("="*70)
                print("優化策略：")
                print("  0. 質量問題修復（自環、重複、缺失來源）")
                print("  1. 實體對齊合併（去重）")
                print("  2. 弱連接實體全局關係推理（度數1-3）")
                print("  3. 假設性問題關係密集化（低密度Chunks）")
                print("  4. 基礎關係強化（二次抽取）")
                print("  5. 孤立點清理")
                print()
                print("⚠️  注意：只對現有實體建立關係，不創建新實體")
                print()
                
                # 讓用戶選擇執行哪些策略
                print("請選擇執行策略（可多選，用逗號分隔，如 0,1,2,3）：")
                strategy_choice = input("策略選項 (0-5 或 'all'): ").strip().lower()
                
                if strategy_choice == "":
                    print("❌ 已取消")
                    continue
                
                # 解析選擇
                if strategy_choice == "all":
                    strategies = [0, 1, 2, 3, 4, 5]
                else:
                    try:
                        strategies = [int(s.strip()) for s in strategy_choice.split(",")]
                    except:
                        print("❌ 無效輸入")
                        continue
                
                confirm = input("確認執行? (yes/no): ").strip().lower()
                if confirm != "yes":
                    print("❌ 已取消")
                    continue
                
                from src.optimizer import GraphOptimizer
                try:
                    optimizer = GraphOptimizer(
                        driver=driver,
                        client=ollama_client,
                        model=CONFIG["models"]["llm_model"]
                    )
                    
                    print("\n" + "="*70)
                    print("開始執行優化流程...")
                    print("="*70)
                    
                    # 執行優化前診斷
                    print("\n📊 優化前狀態...")
                    inspector = GraphInspector(driver)
                    before_stats = inspector.run_basic_diagnosis(verbose=False)
                    
                    # 定義自動停止門檻
                    thresholds = {
                        'min_density': 2.0,           # 最小密度
                        'max_weak_percent': 30.0,     # 弱連接實體比例上限
                        'max_isolated_percent': 5.0,  # 孤立實體比例上限
                        'max_self_loops': 0,          # 自環關係上限
                        'max_duplicates': 0,          # 重複關係上限
                        'max_empty_chunks': 100       # 缺失來源標記上限
                    }
                    
                    print("\n🎯 自動停止門檻設定：")
                    print(f"  • 最小密度：{thresholds['min_density']}")
                    print(f"  • 弱連接實體比例上限：{thresholds['max_weak_percent']}%")
                    print(f"  • 孤立實體比例上限：{thresholds['max_isolated_percent']}%")
                    print(f"  • 自環關係上限：{thresholds['max_self_loops']}")
                    print(f"  • 重複關係上限：{thresholds['max_duplicates']}")
                    print(f"  • 缺失來源標記上限：{thresholds['max_empty_chunks']}")
                    
                    # 執行選定的策略
                    if 0 in strategies:
                        print("\n🔧 策略 0：質量問題修復")
                        quality_results = optimizer.fix_quality_issues()
                        print(f"  • 移除自環關係：{quality_results['self_loops_removed']}")
                        print(f"  • 合併重複關係：{quality_results['duplicate_relations_merged']}")
                        print(f"  • 修復缺失來源：{quality_results['empty_chunks_fixed']}")
                        
                        # 檢查質量門檻
                        if (quality_results['self_loops_removed'] == 0 and 
                            quality_results['duplicate_relations_merged'] == 0 and 
                            quality_results['empty_chunks_fixed'] <= thresholds['max_empty_chunks']):
                            print("  ✅ 質量問題已達標！")
                    
                    if 1 in strategies:
                        print("\n🧩 策略 1：實體對齊合併")
                        optimizer.merge_synonym_entities()
                    
                    if 2 in strategies:
                        print("\n🧠 策略 2：弱連接實體全局關係推理")
                        infer_results = optimizer.infer_global_relations(
                            min_degree=1,
                            max_degree=3,
                            max_inferences_per_entity=5,
                            batch_size=10
                        )
                        print(f"  • 處理實體數：{infer_results['processed_entities']}")
                        print(f"  • 推理關係數：{infer_results['inferred_relations']}")
                    
                    if 3 in strategies:
                        print("\n💡 策略 3：假設性問題關係密集化")
                        densify_results = optimizer.densify_relations_with_questions(
                            dataset_id=CONFIG["infrastructure"]["dataset_id"],
                            target_chunks=100,
                            temperature=0.0
                        )
                        print(f"  • 處理 Chunks：{densify_results['processed_chunks']}")
                        print(f"  • 新增關係：{densify_results['new_relations']}")
                    
                    if 4 in strategies:
                        print("\n🔗 策略 4：基礎關係強化")
                        optimizer.enhance_connectivity(CONFIG["infrastructure"]["dataset_id"])
                    
                    if 5 in strategies:
                        print("\n✂️  策略 5：孤立點清理")
                        optimizer.prune_isolated_nodes()
                    
                    # 執行優化後診斷
                    print("\n📊 優化後狀態...")
                    after_stats = inspector.run_basic_diagnosis(verbose=False)
                    
                    # 獲取質量統計
                    quality_stats = inspector.check_quality_issues()
                    
                    # 對比結果
                    print("\n" + "="*70)
                    print("📈 優化效果對比")
                    print("="*70)
                    print(f"實體數：{before_stats['entities']:,} → {after_stats['entities']:,} ({after_stats['entities']-before_stats['entities']:+,})")
                    print(f"關係數：{before_stats['relation_count']:,} → {after_stats['relation_count']:,} ({after_stats['relation_count']-before_stats['relation_count']:+,})")
                    print(f"密度：{before_stats['density']:.3f} → {after_stats['density']:.3f} ({after_stats['density']-before_stats['density']:+.3f})")
                    print(f"平均度數：{before_stats['avg_degree']:.2f} → {after_stats['avg_degree']:.2f} ({after_stats['avg_degree']-before_stats['avg_degree']:+.2f})")
                    
                    # 檢查是否達到門檻
                    print("\n" + "="*70)
                    print("🎯 門檻達成檢查")
                    print("="*70)
                    
                    thresholds_met = []
                    thresholds_not_met = []
                    
                    # 檢查密度
                    if after_stats['density'] >= thresholds['min_density']:
                        thresholds_met.append(f"✅ 密度：{after_stats['density']:.3f} ≥ {thresholds['min_density']}")
                    else:
                        thresholds_not_met.append(f"❌ 密度：{after_stats['density']:.3f} < {thresholds['min_density']}")
                    
                    # 檢查弱連接實體比例
                    weak_percent = (quality_stats['weak_entities'] / after_stats['entities'] * 100) if after_stats['entities'] > 0 else 0
                    if weak_percent <= thresholds['max_weak_percent']:
                        thresholds_met.append(f"✅ 弱連接實體：{weak_percent:.1f}% ≤ {thresholds['max_weak_percent']}%")
                    else:
                        thresholds_not_met.append(f"❌ 弱連接實體：{weak_percent:.1f}% > {thresholds['max_weak_percent']}%")
                    
                    # 檢查孤立實體比例
                    isolated_percent = (quality_stats['isolated_entities'] / after_stats['entities'] * 100) if after_stats['entities'] > 0 else 0
                    if isolated_percent <= thresholds['max_isolated_percent']:
                        thresholds_met.append(f"✅ 孤立實體：{isolated_percent:.1f}% ≤ {thresholds['max_isolated_percent']}%")
                    else:
                        thresholds_not_met.append(f"❌ 孤立實體：{isolated_percent:.1f}% > {thresholds['max_isolated_percent']}%")
                    
                    # 檢查質量問題
                    if quality_stats['self_loops'] <= thresholds['max_self_loops']:
                        thresholds_met.append(f"✅ 自環關係：{quality_stats['self_loops']} ≤ {thresholds['max_self_loops']}")
                    else:
                        thresholds_not_met.append(f"❌ 自環關係：{quality_stats['self_loops']} > {thresholds['max_self_loops']}")
                    
                    if quality_stats['duplicate_relations'] <= thresholds['max_duplicates']:
                        thresholds_met.append(f"✅ 重複關係：{quality_stats['duplicate_relations']} ≤ {thresholds['max_duplicates']}")
                    else:
                        thresholds_not_met.append(f"❌ 重複關係：{quality_stats['duplicate_relations']} > {thresholds['max_duplicates']}")
                    
                    if quality_stats['empty_chunks'] <= thresholds['max_empty_chunks']:
                        thresholds_met.append(f"✅ 缺失來源：{quality_stats['empty_chunks']} ≤ {thresholds['max_empty_chunks']}")
                    else:
                        thresholds_not_met.append(f"❌ 缺失來源：{quality_stats['empty_chunks']} > {thresholds['max_empty_chunks']}")
                    
                    # 顯示結果
                    for item in thresholds_met:
                        print(item)
                    for item in thresholds_not_met:
                        print(item)
                    
                    print("="*70)
                    
                    if len(thresholds_not_met) == 0:
                        print("\n🎉 所有門檻已達成！優化自動停止。")
                    else:
                        print(f"\n⚠️  還有 {len(thresholds_not_met)} 個門檻未達成，建議繼續優化。")
                    
                    print("\n✅ Phase 3b 優化完成！")
                    
                except Exception as e:
                    print(f"\n❌ 優化失敗: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "5":
                # Phase 4: 检索消融实验
                print("\n" + "="*70)
                print("🧪 Phase 4: 检索消融实验")
                print("="*70)
                
                if not QUESTION_DATASET_PATH.exists():
                    print(f"❌ 问题数据集不存在: {QUESTION_DATASET_PATH}")
                    continue
                
                print(f"📚 问题数据集: {QUESTION_DATASET_PATH}")
                print(f"🎯 测试配置:")
                print(f"   Hops: {CONFIG['retrieval_grid']['hop_counts']}")
                print(f"   Top-K: {CONFIG['retrieval_grid']['top_k_values']}")
                print(f"   最多问题数: {CONFIG['retrieval_grid']['max_questions']}")
                print()
                
                confirm = input("确认运行完整实验? (yes/no): ").strip().lower()
                if confirm != "yes":
                    print("❌ 已取消")
                    continue
                
                try:
                    runner = RetrievalAblationRunner(driver, ollama_client)
                    results = runner.run_experiment(
                        questions_path=QUESTION_DATASET_PATH,
                        hop_values=CONFIG['retrieval_grid']['hop_counts'],
                        top_k_values=CONFIG['retrieval_grid']['top_k_values'],
                        max_questions=CONFIG['retrieval_grid']['max_questions']
                    )
                    print("\n✅ 实验完成！")
                except Exception as e:
                    print(f"\n❌ 实验失败: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "6":
                # 快速测试检索
                print("\n" + "="*70)
                print("🎯 快速测试检索")
                print("="*70)
                
                question = input("请输入问题 (直接回车使用默认问题): ").strip()
                if not question:
                    question = "What are the symptoms of goat disease?"
                    print(f"  使用默认问题: {question}")
                
                try:
                    test_retrieval(driver, ollama_client, question)
                except Exception as e:
                    print(f"\n❌ 测试失败: {e}")
                    import traceback
                    traceback.print_exc()
            
            elif choice == "9":
                # 清理数据库
                print("\n" + "="*70)
                print("🗑️  清理数据库")
                print("="*70)
                print()
                print("请选择清理范围:")
                print("  1. 清理当前 dataset")
                print(f"     (dataset_id: {CONFIG['infrastructure']['dataset_id']})")
                print("  2. 清理所有数据 (⚠️  危险操作)")
                print("  0. 取消")
                print()
                
                clean_choice = input("选择 (0-2): ").strip()
                
                if clean_choice == "0":
                    print("❌ 已取消")
                    continue
                elif clean_choice == "1":
                    confirm = input(f"确认清理 dataset '{CONFIG['infrastructure']['dataset_id']}'? (yes/no): ").strip().lower()
                    if confirm == "yes":
                        try:
                            stats = clean_database(driver, CONFIG["infrastructure"]["dataset_id"], clean_all=False)
                            print("\n✅ 清理完成！")
                        except Exception as e:
                            print(f"\n❌ 清理失败: {e}")
                elif clean_choice == "2":
                    confirm = input("⚠️  确认清理所有数据? 输入 'yes' 确认: ").strip().lower()
                    if confirm == "yes":
                        try:
                            stats = clean_database(driver, "", clean_all=True)
                            print("\n✅ 清理完成！")
                        except Exception as e:
                            print(f"\n❌ 清理失败: {e}")
                    else:
                        print("❌ 已取消")
                else:
                    print("❌ 无效选择")
            
            else:
                print("❌ 无效选择，请输入 0-5 或 9")
            
            input("\n按 Enter 继续...")
    
    except KeyboardInterrupt:
        print("\n\n👋 收到中断信号，正在退出...")
    
    finally:
        # 清理资源
        db.close()
        print("✅ 连接已关闭")


if __name__ == "__main__":
    main()
