"""Test that fix_company_scale correctly overrides wrong scale values"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raw_data.ingestor import FileIngestor, fix_company_scale
import pandas as pd

# Test 1: Direct fix_company_scale test
print("=== Test 1: fix_company_scale ===")
df = pd.DataFrame({
    '所属公司': ['Gilead吉利德', '迪哲', 'Merck默克', '辉致', '雅培',
                 '百济神州', 'BMS 百时美施贵宝投资有限公司', '默沙东', '默克雪兰诺',
                 '卫材', '信达', '罗氏', '赛诺菲', '科赴', '欧加隆', '艾伯维', '参天'],
    '公司规模': ['B', 'B', 'A', 'B', 'B',
                'B', 'B', 'B', 'B',
                'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B'],
})

print("Before fix:")
for _, r in df.iterrows():
    print(f"  {r['所属公司']:30s} | {r['公司规模']}")

df = fix_company_scale(df)

print("\nAfter fix:")
a_count = 0
b_count = 0
for _, r in df.iterrows():
    cls = r['公司规模']
    if cls == 'A': a_count += 1
    else: b_count += 1
    print(f"  {r['所属公司']:30s} | {cls}")

print(f"\nA: {a_count}, B: {b_count}")

# Expected: Gilead=A, 辉致=A, 百济神州=A, BMS=A, 默沙东=A, 默克雪兰诺=A, 
#           卫材=A, 信达=A, 罗氏=A, 赛诺菲=A, 科赴=A, Merck=A (stays A)
#           迪哲=B, 雅培=B, 欧加隆=B, 艾伯维=B, 参天=B
expected_a = 12
expected_b = 5
assert a_count == expected_a, f"Expected {expected_a} A-class, got {a_count}"
assert b_count == expected_b, f"Expected {expected_b} B-class, got {b_count}"
print(f"\n[OK] PASS: {expected_a} A-class, {expected_b} B-class as expected")

# Test 2: Load actual data through ingestor
print("\n=== Test 2: Ingest actual summary table ===")
try:
    ingestor = FileIngestor()
    ds = ingestor.ingest_file(
        os.path.join(ingestor.source_dir, '4.2 TA公司整体效率汇总表-职能.xlsx'),
        header_row=1
    )
    for sheet_name, sdf in ds.sheets.items():
        if '公司规模' in sdf.columns and '所属公司' in sdf.columns:
            companies = sdf[['所属公司', '公司规模']].drop_duplicates()
            print(f"Sheet: {sheet_name}")
            a_list = []
            b_list = []
            for _, r in companies.iterrows():
                co = str(r['所属公司'])
                sc = str(r['公司规模'])
                if sc == 'A': a_list.append(co)
                else: b_list.append(co)
            print(f"  A-class ({len(a_list)}): {a_list}")
            print(f"  B-class ({len(b_list)}): {b_list}")
            if len(a_list) > 0:
                print("  [OK] A-class companies found after fix!")
            else:
                print("  [ERR] Still no A-class companies!")
except Exception as e:
    print(f"  ERR: {e}")
