# src/models.py
"""Monkey patch to fix OllamaLLM.invoke method to handle dict responses.
This patch addresses the issue where Ollama client returns a dict instead of an object,
causing an AttributeError when accessing the 'message' attribute.
"""
import os
from typing import List, Optional, Iterable, Dict, Any
from ollama import Client

from neo4j_graphrag.llm.ollama_llm import OllamaLLM
from neo4j_graphrag.llm import LLMResponse

# 保存原始方法
_original_invoke = OllamaLLM.invoke

def _patched_invoke(self, input, message_history=None, system_instruction=None):
    """
    修補版本的 OllamaLLM.invoke 方法
    兼容 Ollama 客戶端的字典返回格式
    """
    # 處理 message_history (如果是對象則轉換為列表)
    if message_history is not None and hasattr(message_history, 'messages'):
        message_history = message_history.messages
    
    response = self.client.chat(
        model=self.model_name,
        messages=self.get_messages(input, message_history, system_instruction),
        **self.model_params,
    )
    
    # 🔧 關鍵修復：兼容字典和對象兩種格式
    if isinstance(response, dict):
        # 舊版 Ollama 返回字典格式
        content = response.get("message", {}).get("content", "")
    elif hasattr(response, 'message'):
        # 新版 Ollama 返回對象格式
        content = response.message.content or ""
    else:
        # 容錯處理
        content = str(response)
    
    return LLMResponse(content=content)

# 應用 monkey patch
OllamaLLM.invoke = _patched_invoke

print("✅ 已修補 OllamaLLM.invoke 方法，支援 Ollama 字典響應格式")
print("   修復問題：'dict' object has no attribute 'message'")
class OllamaVectorEmbedder:
    def __init__(self, client: Client, model: str, max_length: int = 8000):
        """
        Args:
            client: Ollama client
            model: Embedding model name
            max_length: Maximum character length for embeddings (default: 8000)
                       This is a safety limit to prevent "context length exceeded" errors.
        """
        self._client = client
        self._model = model
        self._dimension: Optional[int] = None
        self._max_length = max_length

    def embed_query(self, text: str) -> List[float]:
        # Truncate text if it exceeds max_length to prevent context overflow
        if len(text) > self._max_length:
            print(f"⚠️ 文本長度 {len(text)} 超過限制 {self._max_length}，已截斷")
            text = text[:self._max_length]
        
        try:
            resp = self._client.embeddings(model=self._model, prompt=text or " ")
            return resp["embedding"]
        except Exception as e:
            if "context length" in str(e).lower() or "input length exceeds" in str(e).lower():
                # If still too long, try with even shorter text
                print(f"⚠️ 嵌入失敗，嘗試更短的文本（{self._max_length // 2} 字元）...")
                text = text[:self._max_length // 2]
                resp = self._client.embeddings(model=self._model, prompt=text or " ")
                return resp["embedding"]
            else:
                raise

    def embed_documents(self, texts: Iterable[str]) -> List[List[float]]:
        return [self.embed_query(t) for t in texts]

    @property
    def dimension(self) -> int:
        if self._dimension is None:
            self._dimension = len(self.embed_query("dimension probe"))
        return self._dimension