# Neo4j 5.x 语法兼容性修复

## 问题描述

在 Neo4j 5.x 中，以下 Cypher 语法已被弃用：

```cypher
❌ 旧语法（Neo4j 4.x）
WHERE size((e)--()) < 2
```

错误信息：
```
A pattern expression should only be used in order to test the existence of a pattern. 
It can no longer be used inside the function size(), 
an alternative is to replace size() with COUNT {}.
```

## 解决方案

使用新的 `COUNT { pattern }` 语法：

```cypher
✅ 新语法（Neo4j 5.x）
WHERE COUNT { (e)--() } < 2
```

## 修复的文件

### src/optimizer.py

**Line 419:** 修复弱实体查询

```python
# 旧版
WHERE size((e)--()) < $threshold

# 新版
WHERE COUNT { (e)--() } < $threshold
```

## 语法对比

| 用途 | 旧语法 (4.x) | 新语法 (5.x) | 说明 |
|------|-------------|-------------|------|
| **计数关系** | `size((e)--())` | `COUNT { (e)--() }` | 计算节点的关系数量 |
| **存在性测试** | `WHERE (e)--()` | `WHERE (e)--()` | 无变化，仍然有效 |
| **不存在测试** | `WHERE NOT (e)--()` | `WHERE NOT (e)--()` | 无变化，仍然有效 |
| **列表大小** | `size(list)` | `size(list)` | 无变化，仍然有效 |

## 重要区别

### ✅ 仍然有效的 `size()` 用法：

1. **列表/数组大小**：
```cypher
WHERE size(weak_entities) > 0  -- ✅ 正确
WHERE size(r.chunks) = 0       -- ✅ 正确
WHERE size(e.name) > 50        -- ✅ 正确（字符串长度）
```

2. **存在性模式测试**：
```cypher
WHERE (e)--()           -- ✅ 正确
WHERE NOT (e)--()       -- ✅ 正确
```

### ❌ 不再有效的 `size()` 用法：

**模式表达式计数**：
```cypher
WHERE size((e)--()) < 2     -- ❌ 错误
WHERE size((e)-[r]->()) > 3 -- ❌ 错误
```

应改为：
```cypher
WHERE COUNT { (e)--() } < 2     -- ✅ 正确
WHERE COUNT { (e)-[r]->() } > 3 -- ✅ 正确
```

## 验证测试

运行测试脚本验证修复：

```bash
python test_cypher_syntax.py
```

预期输出：
```
✅ 测试 1: 旧语法失败（预期）
✅ 测试 2: 新语法成功
✅ 测试 3: 完整优化器查询成功
✅ 测试 4: 孤立节点查询成功
```

## 性能影响

`COUNT { pattern }` 语法与旧的 `size(pattern)` 性能相当，不会影响查询速度。

## 其他需要注意的文件

以下文件中也使用了 `size()`，但都是合法用法（列表/字符串大小），无需修改：

- `src/inspector.py` - ✅ 使用 `size(list)` 和 `size(string)`
- `src/optimizer.py` - ✅ 其他 `size()` 用法都是列表操作
- `src/builder.py` - ✅ 无模式计数

## 相关资源

- [Neo4j 5.x 迁移指南](https://neo4j.com/docs/upgrade-migration-guide/current/)
- [Cypher 手册 - COUNT 子句](https://neo4j.com/docs/cypher-manual/current/subqueries/count/)

## 更新日期

2026-01-20

## 提交记录

- Commit: c73a2a6
- Message: "修复 Neo4j 5.x Cypher 语法兼容性问题"
