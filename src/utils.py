# src/utils.py
"""
通用工具函數模組

提供跨模組共享的工具函數：
- 文本處理：chunk_text, normalize_text
- 三元組處理：parse_triples, deduplicate_triples
"""

import re
import json
from typing import List, Dict, Any, Iterable


def normalize_text(value: Any) -> str:
    """
    標準化文字：去除多餘空白
    
    Args:
        value: 任意型別的輸入值
    
    Returns:
        標準化後的字串
    """
    return re.sub(r"\s+", " ", str(value).strip())


def deduplicate_triples(triples: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    三元組去重
    
    Args:
        triples: 三元組列表
    
    Returns:
        去重後的三元組列表
    """
    unique: List[Dict[str, str]] = []
    seen = set()
    for triple in triples:
        key = (triple.get("head"), triple.get("relation"), triple.get("tail"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(triple)
    return unique


def parse_triples(raw: str) -> List[Dict[str, str]]:
    """
    解析 JSON 格式的三元組字串，並進行基本過濾
    
    質量控制規則：
    1. 過濾自環關係（head == tail）
    2. 過濾超長實體名稱（>50字元，可能是句子片段）
    3. 過濾過短實體名稱（<2字元，可能是無意義符號）
    4. 過濾空白或純數字關係類型
    5. 過濾關係類型過長（>30字元）
    6. 過濾代詞等無意義實體
    
    Args:
        raw: JSON 格式的字串（可能包含 Markdown code block）
    
    Returns:
        驗證後的三元組列表
    """
    candidates: List[Dict[str, str]] = []
    payload = None
    
    # 嘗試從 Markdown code block 提取 JSON
    try:
        payload = json.loads(raw)
    except Exception:
        match = re.search(r"\[[\s\S]*\]", raw)
        if match:
            try:
                payload = json.loads(match.group(0))
            except Exception:
                payload = None
    
    if isinstance(payload, list):
        for item in payload:
            head, relation, tail = None, None, None
            
            if isinstance(item, dict):
                head = item.get("head")
                relation = item.get("relation")
                tail = item.get("tail")
            elif isinstance(item, (list, tuple)) and len(item) == 3:
                head, relation, tail = item
            else:
                continue
            
            # 基本類型檢查
            if not all(isinstance(x, str) and x.strip() for x in (head, relation, tail)):
                continue
            
            # 正規化
            head = normalize_text(head)
            relation = normalize_text(relation)
            tail = normalize_text(tail)
            
            # ═══════════════════════════════════════════════════════
            # 質量過濾規則
            # ═══════════════════════════════════════════════════════
            
            # 規則 1：過濾自環關係（實體指向自己）
            if head.lower() == tail.lower():
                continue
            
            # 規則 2：過濾超長實體名稱（可能是句子片段）
            if len(head) > 50 or len(tail) > 50:
                continue
            
            # 規則 3：過濾過短實體名稱（可能是無意義符號）
            if len(head) < 2 or len(tail) < 2:
                continue
            
            # 規則 4：過濾空白或純數字關係類型
            if not relation or relation.isdigit():
                continue
            
            # 規則 5：過濾關係類型過長（可能是句子）
            if len(relation) > 30:
                continue
            
            # 規則 6：過濾常見的無意義實體（可選）
            meaningless_entities = {'it', 'this', 'that', 'these', 'those', 'they', 'them',
                                   '它', '這', '那', '該', '此', '其'}
            if head.lower() in meaningless_entities or tail.lower() in meaningless_entities:
                continue
            
            # 通過所有過濾規則，加入候選列表
            candidates.append({
                "head": head,
                "relation": relation,
                "tail": tail,
            })
    
    return deduplicate_triples(candidates)


def chunk_text(text: str, chunk_size: int, overlap: int) -> List[str]:
    """
    文本切分工具
    
    Args:
        text: 待切分文本
        chunk_size: 每個 chunk 的大小
        overlap: 重疊字元數
    
    Returns:
        切分後的文本片段列表
    
    Raises:
        ValueError: 當 chunk_size <= 0 時
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    
    step = max(1, chunk_size - overlap)
    chunks: List[str] = []
    start = 0
    
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start += step
    
    return chunks
