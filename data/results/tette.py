import pandas as pd

# 讀取 CSV
df = pd.read_csv('retrieval_ablation_20260112_061519.csv')

# 篩選條件：字數 < 40 且 Cosine < 0.85 的題目 (不重複)
# 這裡我們只取 Hop-3 的結果作為基準，因為它是表現最好的
df_filtered = df[(df['hop'] == 3) & (df['top_k'] == 5)].copy()

# 計算參考答案字數
df_filtered['ref_word_count'] = df_filtered['reference_answer'].apply(lambda x: len(str(x).split()))

# 篩選出需要修正的題目
to_fix = df_filtered[
    (df_filtered['ref_word_count'] < 40) & 
    (df_filtered['cosine_similarity'] < 0.85)
].sort_values(by='cosine_similarity')

# 準備匯出的欄位
export_df = to_fix[['question_id', 'question', 'reference_answer', 'predicted_answer', 'cosine_similarity']]
export_df.columns = ['ID', '題目', '目前參考答案 (需擴充)', 'AI 的回答 (可參考其詳細程度)', '語意分數']

# 存成 Excel (如果沒有安裝 openpyxl，會存成 csv)

export_df.to_excel('questions_to_fix.xlsx', index=False)
print("已匯出至 questions_to_fix.xlsx")

export_df.to_csv('questions_to_fix.csv', index=False)
print("已匯出至 questions_to_fix.csv")