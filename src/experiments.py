# src/experiments.py
"""
实验管理模块

负责运行消融实验（Ablation Study）：
- Phase 1: 索引消融（Indexing Ablation）
- Phase 4: 检索消融（Retrieval Ablation）
"""

import time
import json
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from ollama import Client

from config import CONFIG, RESULT_DIR, KNOWLEDGE_BASE_PATH
from src.retrieval import RetrievalEngine
from src.models import OllamaVectorEmbedder
from src.builder import GraphBuilder
from src.database import clean_database
from src.metrics import calculate_f1_score, calculate_exact_match, calculate_cosine_similarity_score, is_effective_answer

# ✅ 配置日誌系統
LOG_FILE = RESULT_DIR / "experiment.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BaseExperimentRunner:
    """實驗基礎類別，提供通用工具"""
    
    def __init__(self, driver, ollama_client):
        self.driver = driver
        self.ollama_client = ollama_client
        self.embedder = OllamaVectorEmbedder(ollama_client, CONFIG["models"]["embed_model"])
        self.engine = RetrievalEngine(driver, ollama_client)

    def _save_results(self, results: List[Dict], prefix: str):
        """
        同時儲存 CSV 和 JSONL
        - CSV: 方便 Excel 查看
        - JSONL: 保留完整格式（換行符、引號），方便程式讀取
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. 儲存 CSV
        df = pd.DataFrame(results)
        csv_path = RESULT_DIR / f"{prefix}_{timestamp}.csv"
        # 使用 utf-8-sig 讓 Excel 開啟不亂碼，escapechar 處理換行
        df.to_csv(csv_path, index=False, encoding='utf-8-sig', escapechar='\\')
        
        # 2. 儲存 JSONL
        jsonl_path = RESULT_DIR / f"{prefix}_{timestamp}.jsonl"
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for record in results:
                json.dump(record, f, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"✅ 結果已保存：")
        logger.info(f"   📂 CSV: {csv_path}")
        logger.info(f"   📂 JSONL: {jsonl_path}")
        
        print(f"\n✅ 結果已保存：")
        print(f"   📂 CSV: {csv_path}")
        print(f"   📂 JSONL: {jsonl_path}")
        
        return df


class RetrievalAblationRunner(BaseExperimentRunner):
    """Phase 4: 檢索消融實驗 (Hop Count / Top-K)"""
    
    def __init__(self, driver, ollama_client):
        super().__init__(driver, ollama_client)
        logger.info("="*70)
        logger.info("🚀 初始化檢索消融實驗管理器")
        logger.info(f"📊 Embedding 模型: {CONFIG['models']['embed_model']}")
        logger.info(f"📊 LLM 模型: {CONFIG['models']['llm_model']}")
        logger.info("="*70)
    
    def run_experiment(
        self,
        questions_path: Path,
        hop_values: Optional[List[int]] = None,
        top_k_values: Optional[List[int]] = None,
        max_questions: Optional[int] = None
    ) -> pd.DataFrame:
        
        if hop_values is None: 
            hop_values = CONFIG["retrieval_grid"]["hop_counts"]
        if top_k_values is None: 
            top_k_values = CONFIG["retrieval_grid"]["top_k_values"]
        if max_questions is None: 
            max_questions = CONFIG["retrieval_grid"]["max_questions"]
        
        logger.info(f"📚 加載問題數據集: {questions_path}")
        print(f"📚 加載問題數據集: {questions_path}")
        
        df_questions = pd.read_csv(questions_path)
        if len(df_questions) > max_questions:
            df_questions = df_questions.head(max_questions)
            logger.warning(f"⚠️  限制到前 {max_questions} 個問題")
            print(f"  ⚠️  限制到前 {max_questions} 個問題")
        
        logger.info(f"✅ 加載 {len(df_questions)} 個問題")
        print(f"  ✅ 加載 {len(df_questions)} 個問題")
        
        all_results = []
        total_experiments = len(hop_values) * len(top_k_values) * len(df_questions)
        completed = 0
        
        print(f"\n🧪 開始 Phase 4 實驗: {total_experiments} 次測試\n")
        logger.info(f"🧪 開始實驗: {total_experiments} 次測試")
        
        for hop in hop_values:
            for top_k in top_k_values:
                exp_name = f"Hop-{hop}_TopK-{top_k}"
                logger.info(f"\n{'='*70}")
                logger.info(f"🎯 實驗配置: {exp_name}")
                logger.info(f"   Hop={hop} {'(Baseline - Vector Only)' if hop == 0 else ''}, Top-K={top_k}")
                logger.info("="*70)
                
                print(f"{'='*70}")
                print(f"🎯 實驗配置: {exp_name}")
                print(f"   Hop={hop} {'(Baseline - Vector Only)' if hop == 0 else ''}, Top-K={top_k}")
                print("="*70)
                
                exp_start_time = time.time()
                
                for idx, row in df_questions.iterrows():
                    question = row.get('question', row.get('Question', ''))
                    reference_answer = row.get('answer', row.get('Answer', None))
                    
                    try:
                        result = self.engine.run_qa(
                            question=question,
                            hop=hop,
                            top_k=top_k,
                            reference_answer=reference_answer,
                            verbose=False
                        )
                        
                        # ✅ 新增：計算評估指標
                        f1_score = 0.0
                        exact_match = 0
                        cosine_sim = 0.0
                        is_effective = 0
                        
                        if reference_answer:
                            f1_score = calculate_f1_score(result.predicted_answer, reference_answer)
                            exact_match = calculate_exact_match(result.predicted_answer, reference_answer)
                            cosine_sim = calculate_cosine_similarity_score(result.predicted_answer, reference_answer, self.embedder)
                        
                        is_effective = 1 if is_effective_answer(result.predicted_answer) else 0
                        
                        # 记录结果
                        all_results.append({
                            "experiment": exp_name,
                            "hop": hop,
                            "top_k": top_k,
                            "question_id": idx,
                            "question": question,
                            "reference_answer": reference_answer,
                            "predicted_answer": result.predicted_answer,
                            "num_chunks": result.num_chunks,
                            "latency_ms": result.inference_latency_ms,
                            # ✅ 新增指標欄位
                            "f1_score": f1_score,
                            "exact_match": exact_match,
                            "cosine_similarity": cosine_sim,
                            "is_effective": is_effective
                        })
                        
                        completed += 1
                        
                        # 日誌記錄每個問題
                        logger.info(f"✅ Q{idx} | F1={f1_score:.3f} | Cos={cosine_sim:.3f} | Latency={result.inference_latency_ms:.1f}ms | Effective={is_effective}")
                        
                        # 进度显示（包含指標）
                        if completed % 10 == 0:
                            progress = (completed / total_experiments) * 100
                            logger.info(f"📊 進度: {completed}/{total_experiments} ({progress:.1f}%)")
                            print(f"  ↳ 进度: {completed}/{total_experiments} ({progress:.1f}%) | 最近: F1={f1_score:.2f} Cos={cosine_sim:.2f}")
                    
                    except Exception as e:
                        logger.error(f"❌ Q{idx} 失敗: {str(e)}")
                        print(f"  ⚠️  问题 #{idx} 失败: {e}")
                        all_results.append({
                            "experiment": exp_name,
                            "hop": hop,
                            "top_k": top_k,
                            "question_id": idx,
                            "question": question,
                            "reference_answer": reference_answer,
                            "predicted_answer": f"[Error: {e}]",
                            "num_chunks": 0,
                            "latency_ms": 0.0,
                            # ✅ 錯誤時填入 0 分
                            "f1_score": 0.0,
                            "exact_match": 0,
                            "cosine_similarity": 0.0,
                            "is_effective": 0
                        })
                        completed += 1
                
                exp_duration = time.time() - exp_start_time
                logger.info(f"✅ {exp_name} 完成（耗時 {exp_duration:.1f}s）")
                print(f"  ✅ {exp_name} 完成 ({exp_duration:.1f}s)\n")
        
        # 保存结果
        df_results = self._save_results(all_results, "retrieval_ablation")
        
        # 打印摘要
        self._print_summary(df_results)
        
        return df_results
    
    def _print_summary(self, df_results: pd.DataFrame):
        """✅ 修復版摘要打印"""
        logger.info(f"\n{'='*70}")
        logger.info("📊 實驗摘要 (Average Metrics)")
        logger.info("="*70)
        
        print(f"\n{'='*70}")
        print("📊 实验摘要 (Average Metrics)")
        print("="*70)
        
        # ✅ 修正：先過濾掉失敗的測試 (latency_ms = 0.0) 再計算平均延遲
        success_df = df_results[df_results['latency_ms'] > 0]
        failed_count = len(df_results) - len(success_df)
        
        if failed_count > 0:
            logger.warning(f"⚠️  有 {failed_count} 個測試失敗（延遲統計已排除）")
            print(f"⚠️  注意: 有 {failed_count} 個測試失敗（延遲統計已排除）\n")
        
        # 按 hop 和 top_k 分组统计（包含評估指標）
        summary = df_results.groupby(['hop', 'top_k']).agg({
            'f1_score': 'mean',
            'cosine_similarity': 'mean',
            'is_effective': 'mean',  # 有效回答率
            'question_id': 'count'
        }).round(3)
        
        # 單獨計算平均延遲（只算成功的）
        if len(success_df) > 0:
            latency_summary = success_df.groupby(['hop', 'top_k'])['latency_ms'].mean().round(1)
            summary['Avg_Latency_ms'] = latency_summary
        else:
            summary['Avg_Latency_ms'] = 0.0
        
        # ✅ 修正：直接使用當前欄位順序，不再重新命名
        summary.columns = ['Avg_F1', 'Avg_Cosine', 'Effective_Rate', 'Num_Questions', 'Avg_Latency_ms']
        
        # 輸出到日誌和控制台
        summary_str = summary.to_string()
        for line in summary_str.split('\n'):
            logger.info(line)
        
        print(summary)
        print("="*70)
        print("\n💡 提示：Effective_Rate 代表模型給出有效回答（非拒答）的比例")
        logger.info("="*70)
        logger.info("💡 提示：Effective_Rate 代表模型給出有效回答（非拒答）的比例")


class IndexingAblationRunner(BaseExperimentRunner):
    """Phase 1: 索引消融實驗 (Chunk Size / Overlap)"""
    
    def run_experiment(
        self, 
        text_path: Path, 
        chunk_configs: List[Dict[str, int]], 
        questions_path: Path,
        max_questions: int = 150
    ):
        print(f"\n🚀 開始 Phase 1: 索引消融實驗")
        print(f"   配置數: {len(chunk_configs)}")
        print(f"   每組問題數: {max_questions}")
        
        logger.info("="*70)
        logger.info("🚀 開始 Phase 1: 索引消融實驗")
        logger.info(f"   配置數: {len(chunk_configs)}")
        logger.info(f"   每組問題數: {max_questions}")
        logger.info("="*70)
        
        df_questions = pd.read_csv(questions_path)
        if len(df_questions) > max_questions:
            df_questions = df_questions.head(max_questions)
            
        all_results = []
        builder = GraphBuilder(self.driver, self.ollama_client)
        
        for config in chunk_configs:
            chunk_size = config['chunk_size']
            overlap = config['overlap']
            exp_id = f"Chunk-{chunk_size}_Overlap-{overlap}"
            
            print(f"\n{'-'*60}")
            print(f"🏗️  構建配置: {exp_id}")
            print(f"{'-'*60}")
            
            logger.info(f"\n{'-'*60}")
            logger.info(f"🏗️  構建配置: {exp_id}")
            logger.info(f"{'-'*60}")
            
            # 1. 清空資料庫
            print("🗑️  清空資料庫...")
            clean_database(self.driver, "", clean_all=True)
            
            # 2. 重建圖譜
            try:
                print(f"🔨 重建圖譜 (Chunk={chunk_size}, Overlap={overlap})...")
                builder.build_graph(text_path, chunk_size=chunk_size, overlap=overlap)
            except Exception as e:
                print(f"❌ 建圖失敗: {e}")
                logger.error(f"❌ 建圖失敗: {e}")
                continue
                
            # 3. 執行 QA 評測 (固定使用 Hop=2, TopK=10 作為基準)
            print(f"📝 執行 QA 評測...")
            for idx, row in df_questions.iterrows():
                question = row.get('question', '')
                reference = row.get('answer', row.get('reference_answer', ''))
                
                try:
                    # 使用 RetrievalEngine 進行回答
                    qa_result = self.engine.run_qa(
                        question=question, 
                        hop=0,  # 固定參數以比較 Index 效果
                        top_k=10, 
                        reference_answer=reference,
                        verbose=False
                    )
                    
                    # 計算指標
                    f1 = calculate_f1_score(qa_result.predicted_answer, reference)
                    cos = calculate_cosine_similarity_score(qa_result.predicted_answer, reference, self.embedder)
                    
                    all_results.append({
                        "timestamp": datetime.now().isoformat(),
                        "experiment_id": exp_id,
                        "chunk_size": chunk_size,
                        "overlap": overlap,
                        "question_id": idx,
                        "question": question,
                        "reference_answer": reference,
                        "predicted_answer": qa_result.predicted_answer,
                        "f1_score": f1,
                        "cosine_similarity": cos,
                        "num_chunks": qa_result.num_chunks,
                        "latency_ms": qa_result.inference_latency_ms
                    })
                    print(f"   Q{idx} Cos={cos:.2f}", end='\r')
                    logger.info(f"✅ Q{idx} | F1={f1:.3f} | Cos={cos:.3f}")
                    
                except Exception as e:
                    print(f"   ⚠️ QA Error: {e}")
                    logger.error(f"❌ Q{idx} Error: {e}")
            
            print()
            
            # ⚠️  每個配置完成後清空知識庫
            print(f"🗑️  清空知識庫（準備下一個配置）...")
            logger.info(f"🗑️  清空知識庫（準備下一個配置）...")
            clean_database(self.driver, "", clean_all=True)

        # 4. 儲存結果
        df_results = self._save_results(all_results, "indexing_ablation")
        self._print_summary(df_results)
        
        return df_results
        
    def _print_summary(self, df: pd.DataFrame):
        print(f"\n{'='*70}")
        print("📊 Phase 1 實驗摘要 (Indexing Strategy)")
        print("="*70)
        
        logger.info(f"\n{'='*70}")
        logger.info("📊 Phase 1 實驗摘要 (Indexing Strategy)")
        logger.info("="*70)
        
        # Check if DataFrame is empty or missing required columns
        required_cols = ['chunk_size', 'overlap', 'f1_score', 'cosine_similarity', 'num_chunks']
        if df.empty or not all(col in df.columns for col in required_cols):
            msg = "⚠️ 無有效結果可顯示（所有配置均失敗）"
            print(msg)
            logger.warning(msg)
            print("="*70)
            return
        
        summary = df.groupby(['chunk_size', 'overlap']).agg({
            'f1_score': 'mean',
            'cosine_similarity': 'mean',
            'num_chunks': 'mean'
        }).round(3)
        
        summary.columns = ['Avg_F1', 'Avg_Cos', 'Avg_Retrieved']
        
        summary_str = summary.to_string()
        for line in summary_str.split('\n'):
            logger.info(line)
        
        print(summary)
        print("="*70)
