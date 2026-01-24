# ============================================================
# 📋 第 1 步：配置設定 (Configuration Setup)
# ============================================================

import os
from pathlib import Path
from neo4j import GraphDatabase

# 基礎路徑
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
KNOWLEDGE_BASE_PATH = DATA_DIR / "goat_data_text collection-1.2-eng.txt"
QUESTION_DATASET_PATH = DATA_DIR / "topic-dataset(multi-hop).csv"
RESULT_DIR = DATA_DIR / "results"
RESULT_DIR.mkdir(parents=True, exist_ok=True)


# 🔥 自定義三元組抽取 Prompt（高密度知識圖譜）
TRIPLE_PROMPT_TEMPLATE = """
You are an expert knowledge graph engineer. Your task is to extract **explicit and implicit semantic triples** from the text to build a high-density knowledge graph in {language}.

🎯 **Core Objectives (Target Density > 1.8)**:
1. **Zero Isolated Nodes**: Ensure every entity has 2+ connections. Transform weak entities into connected hubs.
2. **Deep Implicit Mining**: Extract causal, functional, and attribute relationships hidden within and across sentences.
3. **Strict Relation Types**: Use specific predicates (e.g., 'causes', 'contains') instead of vague ones (e.g., 'related').
4. **Attribute as Relations**: Treat numbers, states, time, and types as relation tails (e.g., (goat, weight_is, 45kg)).

═══════════════════════════════════════════════════════════════════

## 🛠️ Extraction Strategy Checklist (Must Execute)

### 1. 🔍 Explicit & Implicit Relationship Mining
* **Layer 1 (Explicit)**: Extract directly stated relations (A causes B).
* **Layer 2 (Intra-sentence Implicit)**: Infer hidden links (Subject → Action → Outcome).
    * *Example*: "Vitamin A deficiency causes night blindness." → Extract (Vitamin_A_deficiency, causes, night_blindness) AND (night_blindness, symptom_of, Vitamin_A_deficiency).
* **Layer 3 (Cross-sentence Implicit)**: Connect entities across sentences via shared context.
    * *Example*: "Goats lack Vitamin A. It causes blindness." → Connect (goat, deficient_in, Vitamin_A) AND (Vitamin_A, prevents, blindness).

### 2. 🔢 Attribute & Data Extraction (Crucial for Density)
* **Numerical**: (feed, protein_content_is, 18%), (goat, weight_is, 45kg)
* **State/Characteristic**: (sick_goat, state_is, lethargic), (lesion, color_is, red)
* **Time/Frequency**: (treatment, duration_is, 7_days), (medication, frequency_is, twice_daily)
* **Classification**: (goat, breed_is, Boer), (pneumonia, type_is, respiratory_disease)

### 3. 🔗 Coreference Resolution (Mandatory)
* **Resolve Pronouns**: Replace 'it', 'this', 'that', 'the animal' with the specific entity name.
    * *Bad*: (it, causes, death)
    * *Good*: (viral_infection, causes, death)
* **Restore Omitted Subjects**: If a sentence starts with a verb, link it to the subject from the previous sentence.

### 4. 📝 Standardized Relation Types (Use These Verbs)
* **Causality**: causes, leads_to, triggers, induces, results_in, prevents, inhibits
* **Composition**: contains, comprised_of, part_of, ingredient_is
* **Attribute**: weight_is, length_is, color_is, state_is, located_at, occurs_at
* **Hierarchy**: is_a, belongs_to, type_of, classified_as
* **Function**: used_for, treats, improves, requires, depends_on
* **🚫 BANNED**: related_to, associated_with, has, is (unless 'is_a'), involving.

═══════════════════════════════════════════════════════════════════

## ✅ Output Format
Output **ONLY** a JSON array of triples. No markdown, no explanations.

**Example**:
[
  {{"head": "goat", "relation": "deficient_in", "tail": "vitamin_A"}},
  {{"head": "vitamin_A_deficiency", "relation": "causes", "tail": "night_blindness"}},
  {{"head": "night_blindness", "relation": "symptom_of", "tail": "nutritional_deficiency"}},
  {{"head": "goat", "relation": "weight_is", "tail": "45kg"}}
]

**Text to Extract**:
{chunk}
"""

CONFIG = {
    # ==========================================
    # A. 環境與基礎設施 (使用者提供)
    # ==========================================
    "infrastructure": {
        "neo4j_uri": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        "neo4j_auth": (
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "neo4jgoat")
        ),
        "ollama_host": os.environ.get("OLLAMA_HOST", "http://localhost:11434"),
        "dataset_id": KNOWLEDGE_BASE_PATH.stem.replace(" ", "_") if KNOWLEDGE_BASE_PATH.exists() else "goat_kb_v1",
        "vector_index_name": "chunk_embeddings",
        "fulltext_index_name": "chunk_text_fts",
    },

    # ==========================================
    # B. 模型設定
    # ==========================================
    "models": {
        "llm_model": "deepseek-r1:14b-qwen-distill-q4_K_M",
        "graph_create_model": "deepseek-r1:8b-llama-distill-q4_K_M",
        "embed_model": "nomic-embed-text:latest",
        "answer_language": "english"
    },

    # ==========================================
    # C. 生成參數（優化以避免 CUDA OOM）
    # ==========================================
    "generation": {
        "temperature": 0.7,
        "max_questions": 200,      # 生成问题数量
        "context_window": 4096,
        "batch_size": 10,          # 批次大小
        "max_workers": 2           # 🚀 并行线程数（本机 GPU: 2-4, API: 8-10）
    },

    # ==========================================
    # D. 第一階段：索引消融網格（簡化測試）
    # ==========================================
    "indexing_grid": [
        #{"chunk_size": 128, "overlap": 16},
        #{"chunk_size": 128, "overlap": 32},
        #{"chunk_size": 256, "overlap": 32},
        #{"chunk_size": 256, "overlap": 64},
        #{"chunk_size": 512, "overlap": 128},
        #{"chunk_size": 512, "overlap": 256},
        {"chunk_size": 1024, "overlap": 128},
        {"chunk_size": 1024, "overlap": 256},
        {"chunk_size": 2048, "overlap": 256},  
        {"chunk_size": 2048, "overlap": 512},
        {"chunk_size": 4096, "overlap": 512}, 
        {"chunk_size": 4096, "overlap": 1024},
        {"chunk_size": 8192, "overlap": 1024},
        {"chunk_size": 8192, "overlap": 2048},
    ],
    "optimal_indexing": {"chunk_size":128, "overlap": 16},

    # ==========================================
    # E. 第三階段：圖譜優化參數
    # ==========================================
    "optimization": {
        "hub_threshold_percentile": 95,
        "max_iterations": 1,
        "quality_threshold": 2.5,
        # 🚀 並行處理配置（加速版優化器）
        "max_workers": 2,  # GPU 本地運行建議 2-4，API 服務可設 8-10
    },

    # ==========================================
    # F. 檢索配置（統一管理）
    # ==========================================
    "retrieval": {
        "hop_counts": [0, 1, 2, 3],      # ✅ 0=Baseline (Vector Only), 1-3=Graph RAG
        "top_k_values": [5, 10, 15],     # 返回前 k 個 chunks
        "max_nodes_per_hop": 10,         # 🔥 修正：改为单个整数值（每跳最多扩展的实体数）
        "decay_factor": 0.7,             # 🔥 修正：改为单个浮点数（关联 chunk 的分数衰减系数）
    },

    # ==========================================
    # G. 第四階段：檢索消融網格（簡化測試）
    # ==========================================
    "retrieval_grid": {
        "hop_counts": [0, 1, 2, 3],      # ✅ 0 作為基準線
        "top_k_values": [5, 10, 15],
        "max_questions": 200,             # 最多測試問題數
    }
}

print("✅ 配置載入完成")
print(f"📁 知識庫路徑: {KNOWLEDGE_BASE_PATH}")
print(f"📁 問題集路徑: {QUESTION_DATASET_PATH}")
print(f"🔧 Neo4j URI: {CONFIG['infrastructure']['neo4j_uri']}")
print(f"🤖 推論模型: {CONFIG['models']['llm_model']}")
print(f"📊 索引消融實驗組數: {len(CONFIG['indexing_grid'])}")
print(f"📊 檢索消融實驗組數: {len(CONFIG['retrieval_grid']['hop_counts']) * len(CONFIG['retrieval_grid']['top_k_values'])}")
print(f"🔥 自定義三元組抽取 Prompt 已載入（{len(TRIPLE_PROMPT_TEMPLATE)} 字元）")