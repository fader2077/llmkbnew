# LLM Knowledge Base with Graph RAG

## 项目简介

这是一个基于 Graph RAG 的知识库系统，用于山羊疾病诊断和知识检索。

## 主要功能

- **知识图谱构建**：从文本中自动提取实体和关系，构建高密度知识图谱
- **向量索引**：使用 Nomic Embed 模型进行文本嵌入和向量检索
- **图检索增强生成**：结合向量相似度和图结构进行多跳检索
- **消融实验**：支持索引策略和检索策略的自动化消融实验

## 技术栈

- **数据库**: Neo4j（图数据库 + 向量索引）
- **LLM**: DeepSeek-R1 (通过 Ollama)
- **嵌入模型**: Nomic Embed Text
- **框架**: Python, neo4j-graphrag, pandas

## 项目结构

```
.
├── config.py                   # 配置文件
├── main.py                     # 主程序入口
├── src/                        # 核心代码
│   ├── builder.py              # 图谱构建
│   ├── database.py             # 数据库操作
│   ├── experiments.py          # 消融实验
│   ├── retrieval.py            # 检索引擎
│   ├── metrics.py              # 评估指标
│   ├── models.py               # 模型封装
│   └── utils.py                # 工具函数
├── data/                       # 数据文件
│   ├── goat_data_text collection-1.2-eng12816.txt  # 知识库
│   ├── topic-dataset(multi-hop).csv                # 问题集
│   └── results/                # 实验结果
└── *.ipynb                     # Jupyter notebooks

```

## 环境要求

- Python 3.8+
- Neo4j 5.0+
- Ollama (用于运行 LLM 和嵌入模型)

## 安装依赖

```bash
pip install neo4j pandas numpy scikit-learn ollama neo4j-graphrag
```

## 配置

编辑 `config.py` 文件，设置：

- Neo4j 连接信息
- Ollama 服务地址
- 模型选择
- 实验参数

## 使用方法

### 运行完整实验流程

```bash
python main.py
```

### 自定义实验

修改 `config.py` 中的以下部分：

- `indexing_grid`: 索引策略配置
- `retrieval_grid`: 检索策略配置
- `optimal_indexing`: 最优索引配置

## 实验结果

实验结果保存在 `data/results/` 目录下，包括：

- CSV 文件：方便在 Excel 中查看
- JSONL 文件：保留完整格式，方便程序读取

## 主要特性

### 1. 高密度知识图谱

使用定制的三元组抽取 Prompt，目标密度 > 1.8，包括：
- 显式和隐式关系挖掘
- 属性作为关系
- 共指消解
- 标准化关系类型

### 2. 多跳图检索

支持 0-3 跳的图扩展：
- Hop=0: 纯向量检索（基准）
- Hop=1-3: 图增强检索

### 3. 自动化消融实验

- **Phase 1**: 索引策略消融（chunk size, overlap）
- **Phase 2**: 检索策略消融（hop count, top-k）

## 许可证

MIT License

## 作者

fader2077

## 更新日期

2026-01-20
