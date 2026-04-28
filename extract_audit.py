"""
提取审核脱靶数据明细
"""
import os, sys, glob
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from multi_company import ingest_company, IndustryAggregator

# 找到数据源目录 - 包括子目录
data_dir = r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源'
files = glob.glob(os.path.join(data_dir, '*.xlsx'))
files += glob.glob(os.path.join(data_dir, '**', '*.xlsx'), recursive=True)
# 去重
files = list(set(files))

print(f"找到 {len(files)} 个问卷文件")
print("=" * 80)

agg = IndustryAggregator()
for f in files:
    try:
        raw = ingest_company(f)
        name = agg.add_company(raw)
        print(f"  [OK] {name} ({os.path.basename(f)})")
    except Exception as e:
        print(f"  [ERR] {os.path.basename(f)}: {e}")

print("\n" + "=" * 80)
print("运行数据审核...")
audit_log = agg.run_audit()

print(f"\n共发现 {len(audit_log)} 条脱靶数据：\n")

if audit_log:
    df = pd.DataFrame(audit_log)
    # 保存到CSV
    output_path = os.path.join(os.path.dirname(__file__), 'audit_detail.csv')
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"已保存到: {output_path}\n")
    
    # 打印详细表格
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', 30)
    print(df.to_string(index=False))
else:
    print("无脱靶数据")
