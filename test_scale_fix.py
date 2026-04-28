# -*- coding: utf-8 -*-
"""测试公司规模分类修复"""
import sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from multi_company import ingest_company, _classify_scale, KNOWN_COMPANY_SCALE

# 测试 _classify_scale
print("=" * 60)
print("测试 _classify_scale 函数")
print("=" * 60)
test_cases = [
    ('2000以上', '辉瑞', 'A'),
    ('2000以上', 'MSD', 'A'),
    ('2000以上', '百济神州', 'A'),
    ('1000-2000', 'Abbott', 'B'),
    ('1000-2000', 'BMS', 'A'),
    ('1000-2000', '卫材', 'A'),
    ('1000-2000', '欧加隆', 'B'),
    ('1000-2000', '辉致', 'A'),
    ('500-1000', 'AbbVie', 'B'),
    ('500-1000', 'Santen', 'B'),
    ('500-1000', 'MPCN', 'B'),
    ('500-1000', '迪哲', 'B'),
    ('未知', 'Pfizer', 'A'),
    ('', '未知公司', 'B'),
]

all_pass = True
for scale, company, expected in test_cases:
    result = _classify_scale(scale, company)
    status = '[OK]' if result == expected else '[ERR]'
    if result != expected:
        all_pass = False
    print(f"  {status} {company:15s} | 规模={scale:12s} | 预期={expected} | 实际={result}")

print(f"\n{'[OK] All passed!' if all_pass else '[ERR] Some tests failed'}")

# 测试实际文件解析
print("\n" + "=" * 60)
print("测试实际问卷文件解析")
print("=" * 60)

search_paths = [
    r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源\各家数据\TA 效能\TA效能数据2025',
    r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源\各家数据',
]

found_files = []
for base in search_paths:
    if os.path.exists(base):
        for f in os.listdir(base):
            fp = os.path.join(base, f)
            if f.endswith('.xlsx') and not f.startswith('._') and not f.startswith('~$') and os.path.isfile(fp):
                found_files.append(fp)

print(f"Found {len(found_files)} files:")
for fp in found_files:
    print(f"  -> {os.path.basename(fp)}")

for fp in found_files:
    fn = os.path.basename(fp)
    try:
        raw = ingest_company(fp)
        info = raw.get('company_info', {})
        name = info.get('公司名称', '?')
        scale = info.get('公司规模', '?')
        cls = info.get('公司规模分类', '?')
        print(f"  {cls}类 | {name:20s} | 规模={scale:15s} | 文件={fn}")
    except Exception as e:
        print(f"  ERR | {fn}: {e}")
