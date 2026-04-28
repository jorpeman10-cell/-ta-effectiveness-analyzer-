"""
单公司问卷处理脚本
摄入单个公司的原始调研问卷，存入Raw Data，然后跑完整流程
"""
import os
import sys
import json
import datetime
import pandas as pd
import numpy as np

# 确保模块可导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raw_data.ingestor import FileIngestor, RawDataset
from raw_data.column_mapper import ColumnMapper
from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
from scheme.analysis_dimensions import DIMENSION_REGISTRY


def _find_sheet(sheet_names, keyword):
    """模糊匹配sheet名（处理尾部空格差异）"""
    for name in sheet_names:
        if keyword in name.strip() or keyword in name:
            return name
    raise ValueError(f"未找到包含 '{keyword}' 的Sheet")


def clean_value(v):
    """清洗单个值"""
    if pd.isna(v):
        return np.nan
    s = str(v).strip()
    if s in ('不填写', '-', '', 'nan', 'None', 'NaN'):
        return np.nan
    if s.startswith('[Formula:') or s.startswith('[object'):
        return np.nan
    try:
        return float(s)
    except (ValueError, TypeError):
        return s


def ingest_beigene(filepath: str) -> dict:
    """
    摄入百济神州问卷，解析各Sheet为结构化数据
    返回: {sheet_key: DataFrame} 的字典
    """
    print(f"\n{'='*60}")
    print(f"  摄入原始问卷: {os.path.basename(filepath)}")
    print(f"{'='*60}")

    xl = pd.ExcelFile(filepath)
    print(f"  Sheets: {xl.sheet_names}")

    results = {}

    # ===== Sheet: 基本信息 =====
    try:
        df_info = pd.read_excel(filepath, sheet_name='基本信息', header=None)
        company_info = {
            '公司名称': '百济神州',
            '填表人': 'Yolanda Yang',
            '邮件': 'ying.yang@beigene.com',
            'TA运营模式': 'FTE+第三方',
            '公司规模': '2000以上',
            '公司规模分类': 'A',  # 2000以上为A类
        }
        results['company_info'] = company_info
        print(f"  [OK] 基本信息: {company_info['公司名称']} ({company_info['公司规模']})")
    except Exception as e:
        print(f"  [WARN] 基本信息解析失败: {e}")

    # ===== Sheet: 1 TA 公司整体效率分析 =====
    try:
        # 自动匹配sheet名（处理尾部空格差异）
        main_sheet = _find_sheet(xl.sheet_names, '1 TA 公司整体效率分析')
        df_main = pd.read_excel(filepath, sheet_name=main_sheet, header=None)
        parsed = _parse_main_sheet(df_main, company_info)
        if parsed is not None and not parsed.empty:
            results['main_efficiency'] = parsed
            print(f"  [OK] 公司整体效率: {len(parsed)} 行")
    except Exception as e:
        print(f"  [WARN] 公司整体效率解析失败: {e}")

    # ===== Sheet: 1.1 研发 =====
    try:
        rd_sheet = _find_sheet(xl.sheet_names, 'TA部门效率分析-研发')
        df_rd = pd.read_excel(filepath, sheet_name=rd_sheet, header=None)
        parsed_rd = _parse_rd_sheet(df_rd, company_info)
        if parsed_rd is not None and not parsed_rd.empty:
            results['rd_detail'] = parsed_rd
            print(f"  [OK] 研发细分: {len(parsed_rd)} 行")
    except Exception as e:
        print(f"  [WARN] 研发细分解析失败: {e}")

    # ===== Sheet: 1.2 商业 =====
    try:
        df_comm = pd.read_excel(filepath, sheet_name='1.2  TA部门效率分析 -商业', header=None)
        parsed_comm = _parse_commercial_sheet(df_comm, company_info)
        if parsed_comm is not None and not parsed_comm.empty:
            results['commercial_detail'] = parsed_comm
            print(f"  [OK] 商业细分: {len(parsed_comm)} 行")
    except Exception as e:
        print(f"  [WARN] 商业细分解析失败: {e}")

    # ===== Sheet: 2 TA人员配置 =====
    try:
        df_ta = pd.read_excel(filepath, sheet_name='2 TA 人员配置分析', header=None)
        parsed_ta = _parse_ta_config(df_ta, company_info)
        if parsed_ta is not None:
            results['ta_config'] = parsed_ta
            print(f"  [OK] TA人员配置: {len(parsed_ta)} 行")
    except Exception as e:
        print(f"  [WARN] TA人员配置解析失败: {e}")

    # ===== Sheet: 3 综合招聘效能提升 (问卷) =====
    try:
        df_q = pd.read_excel(filepath, sheet_name='3 综合招聘效能提升', header=None)
        parsed_q = _parse_questionnaire(df_q, company_info)
        if parsed_q is not None:
            results['questionnaire'] = parsed_q
            print(f"  [OK] 问卷数据: {len(parsed_q)} 题")
    except Exception as e:
        print(f"  [WARN] 问卷解析失败: {e}")

    print(f"\n  [OK] 摄入完成: {len(results)} 个数据模块")
    return results


def _parse_main_sheet(df, company_info):
    """解析主效率表 (Sheet 1)"""
    # 找到数据头行 (包含 FTE招聘总量 的行)
    header_row = None
    for i in range(min(10, len(df))):
        row_str = ' '.join(str(v) for v in df.iloc[i].values if pd.notna(v))
        if 'FTE招聘总量' in row_str:
            header_row = i
            break

    if header_row is None:
        print("    [WARN] 未找到数据头行")
        return None

    # 标准列名
    std_cols = [
        '编号', '职能', 'FTE新增', 'FTE替换', '三方员工',
        'HR直招', '猎头_人', 'RPO_人', '内推_人', '主动投递', '校招', '内部转岗',
        '招聘周期_天', '外部渠道成本_万', '猎头费_万', '内推费_万', 'RPO费_万',
        'Website费_万', '其他费_万', '校招成本_万', 'EVP成本_万'
    ]

    rows = []
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i].values
        if pd.isna(row[0]) and pd.isna(row[1]):
            continue
        # 跳过空行
        if all(pd.isna(v) or str(v).strip() == '' for v in row):
            continue

        row_data = {}
        for j, col_name in enumerate(std_cols):
            if j < len(row):
                row_data[col_name] = clean_value(row[j])
            else:
                row_data[col_name] = np.nan

        # 添加公司信息
        row_data['所属公司'] = company_info['公司名称']
        row_data['公司规模'] = company_info['公司规模分类']

        rows.append(row_data)

    result = pd.DataFrame(rows)

    # 计算招聘总量
    if 'FTE新增' in result.columns and 'FTE替换' in result.columns:
        result['招聘总量'] = result[['FTE新增', 'FTE替换']].sum(axis=1, min_count=1)

    # 计算外部渠道招聘
    ext_cols = ['猎头_人', 'RPO_人', '内推_人', '主动投递', '校招']
    valid_ext = [c for c in ext_cols if c in result.columns]
    if valid_ext:
        result['外部渠道招聘'] = result[valid_ext].sum(axis=1, min_count=1)

    return result


def _parse_rd_sheet(df, company_info):
    """解析研发细分表"""
    header_row = None
    for i in range(min(10, len(df))):
        row_str = ' '.join(str(v) for v in df.iloc[i].values if pd.notna(v))
        if 'FTE招聘总量' in row_str:
            header_row = i
            break

    if header_row is None:
        return None

    std_cols = [
        '编号', '三级职能', 'FTE新增', 'FTE替换', '三方员工',
        'HR直招', '猎头_人', 'RPO_人', '内推_人', '主动投递', '校招', '内部转岗',
        '招聘周期_天', '外部渠道成本_万', '猎头费_万', '内推费_万', 'RPO费_万'
    ]

    rows = []
    current_parent = ''
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i].values
        if all(pd.isna(v) or str(v).strip() == '' for v in row):
            continue

        row_data = {}
        for j, col_name in enumerate(std_cols):
            if j < len(row):
                row_data[col_name] = clean_value(row[j])
            else:
                row_data[col_name] = np.nan

        # 判断是否为职级行 (VP, Director, Mgr, General)
        func_name = str(row_data.get('三级职能', ''))
        is_level_row = any(kw in func_name for kw in ['VP', 'Director', 'Mgr', 'General Staff'])

        if not is_level_row and func_name and func_name != 'nan':
            current_parent = func_name
            row_data['职级'] = '整体'
        elif is_level_row:
            row_data['三级职能'] = current_parent
            if 'VP' in func_name:
                row_data['职级'] = 'VP and Above'
            elif 'Director' in func_name:
                row_data['职级'] = 'D-ED'
            elif 'Mgr' in func_name:
                row_data['职级'] = 'M-AD'
            elif 'General' in func_name:
                row_data['职级'] = 'General'
        else:
            row_data['职级'] = '整体'

        row_data['所属公司'] = company_info['公司名称']
        row_data['公司规模'] = company_info['公司规模分类']
        row_data['一级职能'] = '研发'

        # 计算招聘总量
        fte_new = row_data.get('FTE新增', np.nan)
        fte_rep = row_data.get('FTE替换', np.nan)
        if pd.notna(fte_new) or pd.notna(fte_rep):
            row_data['招聘总量'] = (fte_new if pd.notna(fte_new) else 0) + (fte_rep if pd.notna(fte_rep) else 0)

        rows.append(row_data)

    return pd.DataFrame(rows) if rows else None


def _parse_commercial_sheet(df, company_info):
    """解析商业细分表"""
    header_row = None
    for i in range(min(10, len(df))):
        row_str = ' '.join(str(v) for v in df.iloc[i].values if pd.notna(v))
        if 'FTE招聘总量' in row_str:
            header_row = i
            break

    if header_row is None:
        return None

    std_cols = [
        '编号', '三级职能', 'FTE新增', 'FTE替换', '三方员工',
        'HR直招', '猎头_人', 'RPO_人', '内推_人', '主动投递', '校招', '内部转岗',
        '招聘周期_天', '外部渠道成本_万', '猎头费_万', '内推费_万', 'RPO费_万'
    ]

    rows = []
    current_parent = ''
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i].values
        if all(pd.isna(v) or str(v).strip() == '' for v in row):
            continue

        row_data = {}
        for j, col_name in enumerate(std_cols):
            if j < len(row):
                row_data[col_name] = clean_value(row[j])
            else:
                row_data[col_name] = np.nan

        func_name = str(row_data.get('三级职能', ''))
        is_level_row = any(kw in func_name for kw in ['VP', 'Director', 'Mgr', 'General Staff'])

        if not is_level_row and func_name and func_name != 'nan':
            current_parent = func_name
            row_data['职级'] = '整体'
        elif is_level_row:
            row_data['三级职能'] = current_parent
            if 'VP' in func_name:
                row_data['职级'] = 'VP and Above'
            elif 'Director' in func_name:
                row_data['职级'] = 'D-ED'
            elif 'Mgr' in func_name:
                row_data['职级'] = 'M-AD'
            elif 'General' in func_name:
                row_data['职级'] = 'General'
        else:
            row_data['职级'] = '整体'

        row_data['所属公司'] = company_info['公司名称']
        row_data['公司规模'] = company_info['公司规模分类']
        row_data['一级职能'] = '商业'

        fte_new = row_data.get('FTE新增', np.nan)
        fte_rep = row_data.get('FTE替换', np.nan)
        if pd.notna(fte_new) or pd.notna(fte_rep):
            row_data['招聘总量'] = (fte_new if pd.notna(fte_new) else 0) + (fte_rep if pd.notna(fte_rep) else 0)

        rows.append(row_data)

    return pd.DataFrame(rows) if rows else None


def _parse_ta_config(df, company_info):
    """解析TA人员配置"""
    rows = []
    for i in range(len(df)):
        row = df.iloc[i].values
        row_str = ' '.join(str(v) for v in row if pd.notna(v))
        if '公司整体' in row_str or '早期研发' in row_str or '临床开发' in row_str or \
           '商业' in row_str or '生产' in row_str or '职能' in row_str:
            func_name = str(row[1]) if len(row) > 1 and pd.notna(row[1]) else ''
            ta_fte = clean_value(row[2]) if len(row) > 2 else np.nan
            ta_3rd = clean_value(row[3]) if len(row) > 3 else np.nan
            if func_name:
                rows.append({
                    '职能': func_name,
                    'TA_FTE': ta_fte,
                    'TA_第三方': ta_3rd,
                    '所属公司': company_info['公司名称'],
                })

    return pd.DataFrame(rows) if rows else None


def _parse_questionnaire(df, company_info):
    """解析问卷数据"""
    rows = []
    for i in range(len(df)):
        row = df.iloc[i].values
        if len(row) >= 3 and pd.notna(row[1]) and pd.notna(row[2]):
            module = str(row[1]) if pd.notna(row[1]) else ''
            question = str(row[2]) if pd.notna(row[2]) else ''
            if any(kw in question for kw in ['1.1', '1.2', '2.1', '2.2', '2.3', '3.1', '3.2',
                                              '3.3', '3.4', '3.5', '4.1', '4.2', '4.3',
                                              '5.1', '5.2', '5.3', '5.4', '6.1', '6.2', '6.3']):
                rows.append({
                    '模块': module,
                    '问题': question,
                    '所属公司': company_info['公司名称'],
                    '公司规模': company_info['公司规模分类'],
                })

    return pd.DataFrame(rows) if rows else None


def compile_to_wiki(raw_data: dict) -> KnowledgeBase:
    """将解析后的原始数据编译为Wiki知识库"""
    kb = KnowledgeBase()
    company = raw_data.get('company_info', {}).get('公司名称', '未知')

    print(f"\n{'='*60}")
    print(f"  编译 {company} 数据 → Wiki知识库")
    print(f"{'='*60}")

    # === 公司整体效率数据 ===
    main_df = raw_data.get('main_efficiency')
    if main_df is not None and not main_df.empty:
        # 找到公司整体行
        overall_row = main_df[main_df['职能'].astype(str).str.contains('公司整体', na=False)]
        if not overall_row.empty:
            row = overall_row.iloc[0]
            total_hire = row.get('招聘总量', np.nan)

            kb.add_entry(KnowledgeEntry(
                entry_id=f'{company}.overview',
                module='公司概览',
                dimension='公司整体招聘数据',
                group_by=['公司'],
                metric_name='招聘总量',
                metric_value={
                    '公司': company,
                    'FTE新增': row.get('FTE新增'),
                    'FTE替换': row.get('FTE替换'),
                    '招聘总量': total_hire,
                    'HR直招': row.get('HR直招'),
                    '猎头': row.get('猎头_人'),
                    '内推': row.get('内推_人'),
                    '内部转岗': row.get('内部转岗'),
                    '招聘周期_天': row.get('招聘周期_天'),
                    '外部渠道成本_万': row.get('外部渠道成本_万'),
                    '猎头费_万': row.get('猎头费_万'),
                },
                formula='直接数据',
                data_source=f'{company}_问卷',
                tags=['公司概览', company],
            ))
            print(f"  [OK] 公司整体: 招聘总量={total_hire}")

        # 各职能数据
        func_rows = main_df[~main_df['职能'].astype(str).str.contains('公司整体|VP|Director|Mgr|General', na=False)]
        func_rows = func_rows[func_rows['职能'].notna()]

        for _, frow in func_rows.iterrows():
            func_name = str(frow['职能'])
            if func_name == 'nan' or not func_name.strip():
                continue

            hire_total = frow.get('招聘总量', np.nan)
            tth = frow.get('招聘周期_天', np.nan)
            cost = frow.get('外部渠道成本_万', np.nan)

            if pd.notna(hire_total) or pd.notna(tth):
                safe_name = func_name.replace(' ', '_').replace('/', '_')[:30]
                kb.add_entry(KnowledgeEntry(
                    entry_id=f'{company}.func.{safe_name}',
                    module='职能招聘数据',
                    dimension=f'{func_name}招聘数据',
                    group_by=['职能'],
                    metric_name='招聘指标',
                    metric_value={
                        '职能': func_name,
                        '招聘总量': hire_total,
                        'FTE新增': frow.get('FTE新增'),
                        'FTE替换': frow.get('FTE替换'),
                        'HR直招': frow.get('HR直招'),
                        '猎头': frow.get('猎头_人'),
                        '内推': frow.get('内推_人'),
                        '招聘周期_天': tth,
                        '外部渠道成本_万': cost,
                        '猎头费_万': frow.get('猎头费_万'),
                    },
                    formula='直接数据',
                    data_source=f'{company}_问卷',
                    tags=['职能', func_name, company],
                ))

        # 各职级数据
        level_rows = main_df[main_df['职能'].astype(str).str.contains('VP|Director|Mgr|General', na=False)]
        for _, lrow in level_rows.iterrows():
            level_name = str(lrow['职能'])
            if 'VP' in level_name:
                std_level = 'VP and Above'
            elif 'Director' in level_name:
                std_level = 'D-ED'
            elif 'Mgr' in level_name:
                std_level = 'M-AD'
            elif 'General' in level_name:
                std_level = 'General'
            else:
                continue

            kb.add_entry(KnowledgeEntry(
                entry_id=f'{company}.level.{std_level}',
                module='职级招聘数据',
                dimension=f'{std_level}招聘数据',
                group_by=['职级'],
                metric_name='招聘指标',
                metric_value={
                    '职级': std_level,
                    '招聘总量': lrow.get('招聘总量'),
                    'FTE新增': lrow.get('FTE新增'),
                    'FTE替换': lrow.get('FTE替换'),
                    '招聘周期_天': lrow.get('招聘周期_天'),
                    '外部渠道成本_万': lrow.get('外部渠道成本_万'),
                    '猎头费_万': lrow.get('猎头费_万'),
                },
                formula='直接数据',
                data_source=f'{company}_问卷',
                tags=['职级', std_level, company],
            ))

        print(f"  [OK] 职能/职级数据已编译")

    # === 研发细分 ===
    rd_df = raw_data.get('rd_detail')
    if rd_df is not None and not rd_df.empty:
        overall_rd = rd_df[rd_df['职级'] == '整体']
        for _, rrow in overall_rd.iterrows():
            func_name = str(rrow.get('三级职能', ''))
            if func_name and func_name != 'nan':
                safe_name = func_name.replace(' ', '_')[:30]
                kb.add_entry(KnowledgeEntry(
                    entry_id=f'{company}.rd.{safe_name}',
                    module='研发细分',
                    dimension=f'研发-{func_name}',
                    group_by=['研发二级职能'],
                    metric_name='招聘指标',
                    metric_value={
                        '二级职能': func_name,
                        '招聘总量': rrow.get('招聘总量'),
                        'FTE新增': rrow.get('FTE新增'),
                        'FTE替换': rrow.get('FTE替换'),
                        '猎头': rrow.get('猎头_人'),
                        '内推': rrow.get('内推_人'),
                        '招聘周期_天': rrow.get('招聘周期_天'),
                        '外部渠道成本_万': rrow.get('外部渠道成本_万'),
                    },
                    formula='直接数据',
                    data_source=f'{company}_问卷_研发',
                    tags=['研发', func_name, company],
                ))
        print(f"  [OK] 研发细分: {len(overall_rd)} 个子职能")

    # === 商业细分 ===
    comm_df = raw_data.get('commercial_detail')
    if comm_df is not None and not comm_df.empty:
        overall_comm = comm_df[comm_df['职级'] == '整体']
        for _, crow in overall_comm.iterrows():
            func_name = str(crow.get('三级职能', ''))
            if func_name and func_name != 'nan':
                safe_name = func_name.replace(' ', '_')[:30]
                kb.add_entry(KnowledgeEntry(
                    entry_id=f'{company}.comm.{safe_name}',
                    module='商业细分',
                    dimension=f'商业-{func_name}',
                    group_by=['商业二级职能'],
                    metric_name='招聘指标',
                    metric_value={
                        '二级职能': func_name,
                        '招聘总量': crow.get('招聘总量'),
                        'FTE新增': crow.get('FTE新增'),
                        'FTE替换': crow.get('FTE替换'),
                        '猎头': crow.get('猎头_人'),
                        '内推': crow.get('内推_人'),
                        '招聘周期_天': crow.get('招聘周期_天'),
                        '外部渠道成本_万': crow.get('外部渠道成本_万'),
                    },
                    formula='直接数据',
                    data_source=f'{company}_问卷_商业',
                    tags=['商业', func_name, company],
                ))
        print(f"  [OK] 商业细分: {len(overall_comm)} 个子职能")

    # === TA人员配置 ===
    ta_df = raw_data.get('ta_config')
    if ta_df is not None and not ta_df.empty:
        for _, trow in ta_df.iterrows():
            func = str(trow.get('职能', ''))
            if func and func != 'nan':
                kb.add_entry(KnowledgeEntry(
                    entry_id=f'{company}.ta.{func[:20]}',
                    module='TA人员配置',
                    dimension=f'TA配置-{func}',
                    group_by=['职能'],
                    metric_name='TA人员数',
                    metric_value={
                        '职能': func,
                        'TA_FTE': trow.get('TA_FTE'),
                        'TA_第三方': trow.get('TA_第三方'),
                    },
                    formula='直接数据',
                    data_source=f'{company}_问卷_TA配置',
                    tags=['TA配置', company],
                ))
        print(f"  [OK] TA人员配置: {len(ta_df)} 条")

    stats = kb.get_statistics()
    print(f"\n  [OK] 编译完成: {stats['total_entries']} 条知识条目")
    return kb


def generate_report(kb: KnowledgeBase, company: str, output_dir: str):
    """生成单公司分析报告"""
    lines = [
        f"# {company} - TA效能分析报告",
        f"\n生成时间: {datetime.datetime.now().strftime('%Y年%m月%d日 %H:%M')}",
        f"\n知识库条目数: {len(kb.entries)}",
        "",
    ]

    # 按模块输出
    for module in sorted(set(e.module for e in kb.entries.values())):
        entries = kb.query_by_module(module)
        lines.append(f"\n## {module}")
        lines.append("")

        for entry in entries:
            lines.append(f"### {entry.dimension}")
            lines.append("")

            val = entry.metric_value
            if isinstance(val, dict):
                lines.append("| 指标 | 值 |")
                lines.append("|------|-----|")
                for k, v in val.items():
                    if pd.notna(v) if not isinstance(v, str) else bool(v):
                        if isinstance(v, float):
                            lines.append(f"| {k} | {v:.2f} |")
                        else:
                            lines.append(f"| {k} | {v} |")
                lines.append("")
            elif isinstance(val, pd.DataFrame):
                lines.append(val.to_markdown(index=False))
                lines.append("")

    # 关键发现
    lines.append("\n## 关键发现")
    lines.append("")

    overview = kb.get_entry(f'{company}.overview')
    if overview and isinstance(overview.metric_value, dict):
        v = overview.metric_value
        total = v.get('招聘总量')
        tth = v.get('招聘周期_天')
        cost = v.get('外部渠道成本_万')
        if total:
            lines.append(f"• **招聘总量**: {total:.0f}人 (新增{v.get('FTE新增', 'N/A')}人 + 替换{v.get('FTE替换', 'N/A')}人)")
        if tth:
            lines.append(f"• **整体招聘周期**: {tth:.1f}天")
        if cost:
            lines.append(f"• **外部渠道总成本**: {cost:.2f}万元")
            if total and total > 0:
                lines.append(f"• **人均招聘成本**: {cost/total:.2f}万元/人")

    # 渠道分析
    if overview and isinstance(overview.metric_value, dict):
        v = overview.metric_value
        total = v.get('招聘总量', 0)
        if total and total > 0:
            hr_direct = v.get('HR直招', 0) or 0
            headhunter = v.get('猎头', 0) or 0
            referral = v.get('内推', 0) or 0
            transfer = v.get('内部转岗', 0) or 0
            lines.append(f"\n### 渠道分布")
            lines.append(f"• HR直招: {hr_direct:.0f}人 ({hr_direct/total:.1%})")
            lines.append(f"• 猎头: {headhunter:.0f}人 ({headhunter/total:.1%})")
            lines.append(f"• 内部推荐: {referral:.0f}人 ({referral/total:.1%})")
            lines.append(f"• 内部转岗: {transfer:.0f}人 ({transfer/total:.1%})")

    report_text = "\n".join(lines)

    # 保存
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, f'{company}_分析报告.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\n  [FILE] 报告已保存: {report_path}")

    return report_text


def validate_data(kb: KnowledgeBase, company: str, output_dir: str):
    """验证数据准确性"""
    print(f"\n{'='*60}")
    print(f"  数据验证与自查")
    print(f"{'='*60}")

    checks = []

    # 1. 检查知识库条目数
    total = len(kb.entries)
    if total >= 10:
        checks.append(('[OK]', '知识库条目数', f'{total} 条，数据充足'))
    else:
        checks.append(('[WARN]', '知识库条目数', f'{total} 条，数据可能不足'))

    # 2. 检查公司整体数据
    overview = kb.get_entry(f'{company}.overview')
    if overview:
        v = overview.metric_value
        total_hire = v.get('招聘总量')
        if pd.notna(total_hire) and total_hire > 0:
            checks.append(('[OK]', '公司整体招聘总量', f'{total_hire:.0f}人'))
        else:
            checks.append(('[ERR]', '公司整体招聘总量', '数据缺失'))

        # 验证渠道人数之和
        hr = v.get('HR直招', 0) or 0
        hh = v.get('猎头', 0) or 0
        ref = v.get('内推', 0) or 0
        transfer = v.get('内部转岗', 0) or 0
        channel_sum = hr + hh + ref + transfer
        if total_hire and total_hire > 0:
            ratio = channel_sum / total_hire
            if 0.8 <= ratio <= 1.2:
                checks.append(('[OK]', '渠道人数一致性', f'渠道合计{channel_sum:.0f} vs 总量{total_hire:.0f} (比值{ratio:.2f})'))
            else:
                checks.append(('[WARN]', '渠道人数一致性', f'渠道合计{channel_sum:.0f} vs 总量{total_hire:.0f} (比值{ratio:.2f})'))
    else:
        checks.append(('[ERR]', '公司整体数据', '未找到'))

    # 3. 检查职能数据
    func_entries = kb.query_by_module('职能招聘数据')
    if func_entries:
        checks.append(('[OK]', '职能数据', f'{len(func_entries)} 个职能'))
    else:
        checks.append(('[WARN]', '职能数据', '缺失'))

    # 4. 检查研发/商业细分
    rd_entries = kb.query_by_module('研发细分')
    comm_entries = kb.query_by_module('商业细分')
    checks.append(('[OK]' if rd_entries else '[WARN]', '研发细分', f'{len(rd_entries)} 个子职能'))
    checks.append(('[OK]' if comm_entries else '[WARN]', '商业细分', f'{len(comm_entries)} 个子职能'))

    # 输出
    lines = ["# 数据验证报告", ""]
    lines.append(f"| 状态 | 检查项 | 结果 |")
    lines.append(f"|------|--------|------|")
    for status, check, result in checks:
        lines.append(f"| {status} | {check} | {result} |")
        print(f"  {status} {check}: {result}")

    validation_text = "\n".join(lines)
    val_path = os.path.join(output_dir, f'{company}_验证报告.md')
    with open(val_path, 'w', encoding='utf-8') as f:
        f.write(validation_text)

    return checks


if __name__ == '__main__':
    # 百济神州问卷路径
    filepath = r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源\2024TA  BeiGene_20250312.xlsx'
    output_dir = r'c:\Users\EDY\.kimi\skills\ta_report_automation\llm_wiki\output_beigene'

    # Step 1: INGEST - 摄入原始数据
    raw_data = ingest_beigene(filepath)

    # Step 2: 保存Raw Data
    os.makedirs(output_dir, exist_ok=True)
    raw_summary = {
        'company': raw_data.get('company_info', {}),
        'modules': list(raw_data.keys()),
        'ingested_at': datetime.datetime.now().isoformat(),
    }
    for key, val in raw_data.items():
        if isinstance(val, pd.DataFrame):
            raw_summary[f'{key}_shape'] = list(val.shape)
            val.to_csv(os.path.join(output_dir, f'raw_{key}.csv'), index=False, encoding='utf-8-sig')

    with open(os.path.join(output_dir, 'raw_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(raw_summary, f, ensure_ascii=False, indent=2, default=str)

    # Step 3: COMPILE - 编译为Wiki
    kb = compile_to_wiki(raw_data)

    # 保存知识库
    kb.export_to_json(os.path.join(output_dir, 'knowledge_base.json'))
    kb_summary = kb.export_summary_markdown()
    with open(os.path.join(output_dir, 'knowledge_base_summary.md'), 'w', encoding='utf-8') as f:
        f.write(kb_summary)

    # Step 4: REPORT - 生成报告
    company = raw_data.get('company_info', {}).get('公司名称', '百济神州')
    report = generate_report(kb, company, output_dir)

    # Step 5: VALIDATE - 验证
    validate_data(kb, company, output_dir)

    print(f"\n{'='*60}")
    print(f"  全流程完成!")
    print(f"  输出目录: {output_dir}")
    print(f"{'='*60}")
