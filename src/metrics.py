import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Any

# ============================================================
# 輔助函數：計算評估指標
# ============================================================

def calculate_f1_score(predicted: str, reference: str) -> float:
    """
    計算 F1 分數 (基於 Token Overlap)
    """
    # 轉為小寫並分詞
    pred_tokens = set(predicted.lower().split())
    ref_tokens = set(reference.lower().split())
    
    if len(pred_tokens) == 0 or len(ref_tokens) == 0:
        return 0.0
    
    # 計算交集
    common = pred_tokens.intersection(ref_tokens)
    
    if len(common) == 0:
        return 0.0
    
    # 計算 Precision 和 Recall
    precision = len(common) / len(pred_tokens)
    recall = len(common) / len(ref_tokens)
    
    # 計算 F1
    f1 = 2 * (precision * recall) / (precision + recall)
    return f1


def calculate_exact_match(predicted: str, reference: str) -> int:
    """
    計算完全匹配（Exact Match）
    """
    if not predicted or not reference:
        return 0
        
    # 正規化：移除多餘空白、轉小寫
    pred_normalized = " ".join(predicted.lower().split())
    ref_normalized = " ".join(reference.lower().split())
    
    return 1 if pred_normalized == ref_normalized else 0


def calculate_cosine_similarity_score(predicted: str, reference: str, embedder: Any) -> float:
    """
    計算語義相似度（Cosine Similarity）
    Args:
        embedder: 必須具有 embed_query(text) -> List[float] 方法的物件
    """
    try:
        if not predicted or not reference:
            return 0.0

        # 使用 embedding 模型產生向量
        pred_embedding = embedder.embed_query(predicted)
        ref_embedding = embedder.embed_query(reference)
        
        # Reshape 為 (1, -1) 以符合 sklearn 輸入格式
        pred_vec = np.array([pred_embedding])
        ref_vec = np.array([ref_embedding])
        
        # 計算 cosine similarity
        similarity = cosine_similarity(pred_vec, ref_vec)[0][0]
        
        return float(similarity)
    except Exception as e:
        print(f"⚠️ Cosine similarity 計算錯誤: {e}")
        return 0.0


def is_effective_answer(answer: str, min_length: int = 10) -> bool:
    """
    判斷答案是否有效（過濾拒絕回答或過短的無效回答）
    """
    if not answer or not isinstance(answer, str):
        return False
    
    # 移除空白後檢查長度
    cleaned = answer.strip()
    
    # 檢查是否為拒絕回答的常見模式
    refusal_patterns = [
        "無法回答", "不知道", "沒有相關", "無相關資訊",
        "cannot answer", "don't know", "no relevant", 
        "i'm sorry", "i am sorry"
    ]
    
    for pattern in refusal_patterns:
        if pattern in cleaned.lower():
            return False
    
    return len(cleaned) >= min_length