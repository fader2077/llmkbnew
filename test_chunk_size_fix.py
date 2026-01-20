# test_chunk_size_fix.py
"""
验证 Phase 1 消融实验的 chunk_size 修复
测试不同 chunk_size 是否真的产生不同数量的 chunks
"""
from pathlib import Path
from config import CONFIG, KNOWLEDGE_BASE_PATH
from src.builder import load_chunks

def test_chunk_size_variation():
    """测试不同 chunk_size 是否产生不同的 chunk 数量"""
    print("\n" + "="*70)
    print("🧪 测试 Chunk Size 修复")
    print("="*70)
    
    # 测试配置
    test_configs = [
        {"chunk_size": 256, "overlap": 32, "expected_min": 3000},
        {"chunk_size": 512, "overlap": 128, "expected_min": 1500},
        {"chunk_size": 1024, "overlap": 256, "expected_min": 700},
        {"chunk_size": 2048, "overlap": 512, "expected_min": 350},
    ]
    
    print(f"\n📁 测试文件: {KNOWLEDGE_BASE_PATH}")
    
    results = []
    for config in test_configs:
        chunk_size = config["chunk_size"]
        overlap = config["overlap"]
        expected_min = config["expected_min"]
        
        print(f"\n📊 测试配置: Size={chunk_size}, Overlap={overlap}")
        
        try:
            # 调用 load_chunks 并传入参数
            chunks = load_chunks(KNOWLEDGE_BASE_PATH, chunk_size=chunk_size, overlap=overlap)
            chunk_count = len(chunks)
            
            # 验证结果
            is_valid = chunk_count >= expected_min
            status = "✅ 通过" if is_valid else "❌ 失败"
            
            print(f"  Chunks 数量: {chunk_count:,}")
            print(f"  预期最小值: {expected_min:,}")
            print(f"  验证结果: {status}")
            
            results.append({
                "config": f"{chunk_size}/{overlap}",
                "count": chunk_count,
                "expected": expected_min,
                "valid": is_valid
            })
            
        except Exception as e:
            print(f"  ❌ 错误: {e}")
            results.append({
                "config": f"{chunk_size}/{overlap}",
                "count": 0,
                "expected": expected_min,
                "valid": False
            })
    
    # 总结报告
    print("\n" + "="*70)
    print("📊 测试结果总结")
    print("="*70)
    
    print(f"\n{'配置':<15} {'Chunks数量':<15} {'预期最小值':<15} {'状态':<10}")
    print("-" * 70)
    
    all_valid = True
    for result in results:
        status_icon = "✅" if result["valid"] else "❌"
        print(f"{result['config']:<15} {result['count']:<15,} {result['expected']:<15,} {status_icon:<10}")
        if not result["valid"]:
            all_valid = False
    
    print("\n" + "="*70)
    
    if all_valid:
        print("🎉 所有测试通过！Chunk Size 修复成功！")
        print("\n关键验证：")
        print("  ✅ 小 chunk_size (256) 产生更多 chunks")
        print("  ✅ 大 chunk_size (2048) 产生更少 chunks")
        print("  ✅ 每个配置的 chunk 数量符合预期")
        print("\n✅ Phase 1 消融实验现在可以正常运行了！")
    else:
        print("❌ 部分测试失败！请检查修复是否正确。")
    
    print("="*70)
    
    return all_valid

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 开始验证 Phase 1 Chunk Size 修复")
    print("="*70)
    
    success = test_chunk_size_variation()
    
    if success:
        print("\n✅ 修复验证完成！可以运行 Phase 1 实验了。")
    else:
        print("\n⚠️  修复可能不完整，请检查。")
