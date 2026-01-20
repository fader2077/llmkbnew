# Quick fix for inspector.py
with open('src/inspector.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

with open('src/inspector.py', 'w', encoding='utf-8') as f:
    f.writelines(lines[:477])

print("✅ inspector.py fixed! Kept first 477 lines.")
