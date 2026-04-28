"""
多公司问卷批量处理 + 行业报告生成
支持20家公司问卷上传，生成行业级TA效能分析报告
"""
import os, sys, json, datetime
import pandas as pd
import numpy as np
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_single_company import (
    _find_sheet, clean_value, _parse_main_sheet, _parse_rd_sheet,
    _parse_commercial_sheet, _parse_ta_config, _parse_questionnaire,
    compile_to_wiki
)
from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
from data_validator import DataValidator, validate_and_clean, generate_validation_report


# ============================================================
# 0. 数据审核与Trim（过滤脱靶数据）
# ============================================================
# 合理范围定义：超出范围的数据视为脱靶，将被标记并排除
VALID_RANGES = {
    '招聘总量':       (0, 50000),      # 单个职能招聘量0~50000人
    '招聘周期_天':    (1, 365),         # 招聘周期1~365天
    '外部渠道成本_万': (0, float('inf')),  # 无上限，单位万元
    '猎头费_万':      (0, float('inf')),   # 无上限
    '内推费_万':      (0, float('inf')),   # 无上限
    'RPO费_万':       (0, float('inf')),   # 无上限
    'HR直招':         (0, 50000),
    '猎头_人':        (0, 50000),
    'RPO_人':         (0, 50000),
    '内推_人':        (0, 50000),
    '主动投递':       (0, 50000),
    '校招':           (0, 50000),
    '内部转岗':       (0, 50000),
    'TA_FTE':         (0, 500),
    'TA_第三方':      (0, 500),
}


def trim_outliers(records: list) -> tuple:
    """
    审核并Trim脱靶数据。
    返回: (cleaned_records, audit_log)
    - cleaned_records: 修正后的记录列表
    - audit_log: 审核日志 [(公司, 字段, 原始值, 处理方式)]
    """
    audit_log = []
    cleaned = []

    for rec in records:
        new_rec = dict(rec)
        company = rec.get('公司', '未知')
        func = rec.get('职能', '')
        level = rec.get('职级', '')

        for field, (lo, hi) in VALID_RANGES.items():
            if field not in new_rec:
                continue
            val = pd.to_numeric(new_rec[field], errors='coerce')
            if pd.isna(val):
                continue

            if val < lo or val > hi:
                audit_log.append({
                    '公司': company,
                    '职能': func,
                    '职级': level,
                    '字段': field,
                    '原始值': val,
                    '合理范围': f"[{lo}, {hi}]",
                    '处理': '置为NaN（排除）',
                })
                new_rec[field] = np.nan
            elif val < 0:
                # 负数在招聘数据中通常不合理
                audit_log.append({
                    '公司': company,
                    '职能': func,
                    '职级': level,
                    '字段': field,
                    '原始值': val,
                    '合理范围': f"[{lo}, {hi}]",
                    '处理': '负数置为NaN',
                })
                new_rec[field] = np.nan

        cleaned.append(new_rec)

    return cleaned, audit_log


def generate_audit_report(audit_log: list) -> str:
    """生成审核报告"""
    if not audit_log:
        return "✅ 数据审核通过，未发现脱靶数据。"

    lines = [
        "## 📋 数据审核报告\n",
        f"共发现 **{len(audit_log)}** 条脱靶数据，已自动修正：\n",
        "| 公司 | 职能 | 职级 | 字段 | 原始值 | 合理范围 | 处理 |",
        "|------|------|------|------|--------|----------|------|",
    ]
    for item in audit_log:
        lines.append(
            f"| {item['公司']} | {item['职能']} | {item['职级']} | "
            f"{item['字段']} | {item['原始值']:.2f} | {item['合理范围']} | {item['处理']} |"
        )
    return "\n".join(lines)


# ============================================================
# 1. 通用问卷摄入（自动检测公司名和规模）
# ============================================================
def ingest_company(filepath: str) -> dict:
    """摄入单个公司问卷，返回结构化数据"""
    xl = pd.ExcelFile(filepath)
    results = {}

    # 基本信息
    try:
        info_sheet = _find_sheet(xl.sheet_names, '基本信息')
        df_info = pd.read_excel(filepath, sheet_name=info_sheet, header=None)
        company_name, scale, scale_class = _extract_company_info(df_info)
    except:
        company_name = os.path.basename(filepath).split('_')[0].split('.')[0]
        scale, scale_class = '未知', 'B'

    company_info = {
        '公司名称': company_name,
        '公司规模': scale,
        '公司规模分类': scale_class,
    }
    results['company_info'] = company_info

    # 主效率表
    try:
        main_sheet = _find_sheet(xl.sheet_names, 'TA 公司整体效率分析')
        df = pd.read_excel(filepath, sheet_name=main_sheet, header=None)
        parsed = _parse_main_sheet(df, company_info)
        if parsed is not None and not parsed.empty:
            results['main_efficiency'] = parsed
    except:
        pass

    # 研发
    try:
        rd_sheet = _find_sheet(xl.sheet_names, 'TA部门效率分析-研发')
        df = pd.read_excel(filepath, sheet_name=rd_sheet, header=None)
        parsed = _parse_rd_sheet(df, company_info)
        if parsed is not None and not parsed.empty:
            results['rd_detail'] = parsed
    except:
        pass

    # 商业
    try:
        for s in xl.sheet_names:
            if '商业' in s:
                df = pd.read_excel(filepath, sheet_name=s, header=None)
                parsed = _parse_commercial_sheet(df, company_info)
                if parsed is not None and not parsed.empty:
                    results['commercial_detail'] = parsed
                break
    except:
        pass

    # TA配置
    try:
        ta_sheet = _find_sheet(xl.sheet_names, 'TA 人员配置')
        df = pd.read_excel(filepath, sheet_name=ta_sheet, header=None)
        parsed = _parse_ta_config(df, company_info)
        if parsed is not None:
            results['ta_config'] = parsed
    except:
        pass

    # 问卷
    try:
        q_sheet = _find_sheet(xl.sheet_names, '综合招聘效能')
        df = pd.read_excel(filepath, sheet_name=q_sheet, header=None)
        parsed = _parse_questionnaire(df, company_info)
        if parsed is not None:
            results['questionnaire'] = parsed
    except:
        pass

    return results


# 已知公司规模分类（来自核查总表，作为ground truth）
# A类: ≥1500人; B类: <1500人
KNOWN_COMPANY_SCALE = {
    # A类
    '辉致': 'A', 'Viatris': 'A', '晖致': 'A',
    '辉瑞': 'A', 'Pfizer': 'A',
    '科赴': 'A', 'Kenvue': 'A',
    '诺华': 'A', 'Novartis': 'A',
    '卫材': 'A', 'Eisai': 'A',
    '信达': 'A', 'Innovent': 'A',
    '罗氏': 'A', 'Roche': 'A',
    '百济神州': 'A', 'BeiGene': 'A', 'BeOne': 'A',
    '默沙东': 'A', 'MSD': 'A',
    '赛诺菲': 'A', 'Sanofi': 'A', 'SA': 'A',
    '默克雪兰诺': 'A', 'Merck': 'A',
    'BMS': 'A', '百时美施贵宝': 'A',
    '吉利德': 'A', 'Gilead': 'A', 'GE': 'A',  # GE=6900人，A类（仅提供效能提升部分数据）
    # B类
    '迪哲': 'B', 'Dizal': 'B',
    '雅培': 'B', 'Abbott': 'B',
    '参天': 'B', 'Santen': 'B',
    '欧加隆': 'B', 'Organon': 'B',
    '艾伯维': 'B', 'AbbVie': 'B', 'ABV': 'B',
    'SMPC': 'B', 'MPCN': 'B',
}

# 仅提供部分数据的公司（如只有效能提升问卷，无完整TA效率数据）
PARTIAL_DATA_COMPANIES = {
    'GE': '仅提供效能提升部分数据，无完整TA效率表',
    '吉利德': '仅提供效能提升部分数据，无完整TA效率表',
    'Gilead': '仅提供效能提升部分数据，无完整TA效率表',
}


def _classify_scale(scale_str, company_name=''):
    """
    根据人员规模字符串判断公司分类
    A类: ≥1500人; B类: <1500人
    
    问卷中规模选项: '500以下', '500-1000', '1000-2000', '2000以上'
    """
    import re
    
    # 1. 先查已知公司注册表（最可靠）
    for key, cls in KNOWN_COMPANY_SCALE.items():
        if key in company_name or company_name in key:
            return cls
    
    # 2. 解析规模字符串
    s = str(scale_str).strip()
    
    # "2000以上" / "5000以上" → A
    if '以上' in s:
        nums = re.findall(r'\d+', s)
        if nums and int(nums[0]) >= 1500:
            return 'A'
    
    # "X-Y" 范围格式，如 "1000-2000", "500-1000"
    range_match = re.findall(r'(\d+)\s*[-~到]\s*(\d+)', s)
    if range_match:
        lo, hi = int(range_match[0][0]), int(range_match[0][1])
        # 如果下限 ≥ 1500，肯定是A
        if lo >= 1500:
            return 'A'
        # 如果上限 ≤ 1500，肯定是B
        if hi <= 1500:
            return 'B'
        # 1000-2000 这种跨1500的范围，默认按上限判断为A
        # （因为大多数填1000-2000的公司实际≥1500）
        if hi >= 2000:
            return 'A'
        return 'B'
    
    # 纯数字
    nums = re.findall(r'\d+', s)
    if nums:
        max_num = max(int(n) for n in nums)
        if max_num >= 1500:
            return 'A'
        return 'B'
    
    # 无法判断，默认B
    return 'B'


def _extract_company_info(df):
    """
    从基本信息Sheet提取公司名称和规模
    
    问卷固定格式:
    - Row 3 (index 3): [1.1, 公司名称, <公司名>, ...]
    - Row 10 (index 10): [1.7, 公司人员总体规模, <规模值>]
    """
    company_name = '未知公司'
    scale = '未知'
    scale_class = 'B'

    # 方法1: 基于固定行号提取（最可靠）
    # Excel布局: col[0]=NaN, col[1]=编号, col[2]=标签, col[3]=值
    if len(df) > 10:
        # Row 3: 公司名称 → 值在col[3]
        row3 = df.iloc[3].values
        for v in row3[3:]:  # 从col[3]开始，跳过标签列
            if pd.notna(v) and str(v).strip():
                val = str(v).strip()
                # 排除表头文字
                if not any(k in val for k in ['Revenue', 'Number', 'Clients', 'By ']):
                    company_name = val
                    break
        
        # Row 10: 公司人员总体规模 → 值在col[3]
        row10 = df.iloc[10].values
        for v in row10[3:]:  # 从col[3]开始，跳过标签列
            if pd.notna(v) and str(v).strip():
                scale = str(v).strip()
                break

    # 方法2: 关键字搜索（兜底）
    if company_name == '未知公司' or scale == '未知':
        for i in range(min(20, len(df))):
            row = df.iloc[i].values
            row_str = ' '.join(str(v) for v in row if pd.notna(v))
            
            if company_name == '未知公司' and '公司名称' in row_str:
                for v in row[2:]:
                    if pd.notna(v) and str(v).strip() and '公司名称' not in str(v):
                        val = str(v).strip()
                        if not any(k in val for k in ['Revenue', 'Number', 'Clients', 'By ']):
                            company_name = val
                            break
            
            if scale == '未知' and any(k in row_str for k in ['公司人员', '人员总体规模', '总体规模', '1.7']):
                for v in row[2:]:
                    if pd.notna(v) and str(v).strip():
                        val = str(v).strip()
                        if any(c.isdigit() for c in val) or '以上' in val or '以下' in val:
                            scale = val
                            break

    # 分类
    scale_class = _classify_scale(scale, company_name)

    return company_name, scale, scale_class


# ============================================================
# 2. 多公司数据聚合器
# ============================================================
class IndustryAggregator:
    """行业数据聚合器 - 汇总多家公司数据"""

    def __init__(self):
        self.companies = {}          # {company_name: raw_data}
        self.knowledge_bases = {}    # {company_name: KnowledgeBase}
        self.flat_records = []       # 扁平化记录列表

    def add_company_raw(self, raw_data: dict):
        """添加一家公司到Raw Data层（未验证）"""
        name = raw_data.get('company_info', {}).get('公司名称', '未知')
        if not hasattr(self, 'raw_pool'):
            self.raw_pool = {}
        self.raw_pool[name] = raw_data
        return name

    def validate_all(self):
        """验证所有Raw Data，通过的进入Wiki层"""
        if not hasattr(self, 'raw_pool'):
            return [], [], "无待验证数据"
        raw_list = list(self.raw_pool.values())
        cleaned, results, report = validate_and_clean(raw_list)
        self.validation_results = results
        self.validation_report = report
        # 将验证通过的数据加入Wiki
        for data in cleaned:
            self.add_company(data)
        return cleaned, results, report

    def add_company(self, raw_data: dict):
        """添加一家公司数据到Wiki层（已验证/清洗）"""
        name = raw_data.get('company_info', {}).get('公司名称', '未知')
        self.companies[name] = raw_data
        kb = compile_to_wiki(raw_data)
        self.knowledge_bases[name] = kb
        self._flatten(raw_data, name)
        return name

    def _flatten(self, raw_data, company):
        """将公司数据扁平化为记录"""
        info = raw_data.get('company_info', {})
        scale = info.get('公司规模分类', 'B')

        main_df = raw_data.get('main_efficiency')
        if main_df is None:
            return

        # 公司整体行
        overall = main_df[main_df['职能'].astype(str).str.contains('公司整体', na=False)]
        if not overall.empty:
            row = overall.iloc[0]
            total = row.get('招聘总量', 0) or 0
            if total > 0:
                self.flat_records.append({
                    '公司': company, '规模': scale, '层级': '公司整体',
                    '职能': '公司整体', '职级': '整体',
                    '招聘总量': total,
                    'FTE新增': row.get('FTE新增'), 'FTE替换': row.get('FTE替换'),
                    'HR直招': row.get('HR直招'), '猎头_人': row.get('猎头_人'),
                    'RPO_人': row.get('RPO_人'), '内推_人': row.get('内推_人'),
                    '主动投递': row.get('主动投递'), '校招': row.get('校招'),
                    '内部转岗': row.get('内部转岗'),
                    '招聘周期_天': row.get('招聘周期_天'),
                    '外部渠道成本_万': row.get('外部渠道成本_万'),
                    '猎头费_万': row.get('猎头费_万'),
                    '内推费_万': row.get('内推费_万'),
                    'RPO费_万': row.get('RPO费_万'),
                })

        # 一级职能行
        func_keywords = ['早期研发', '临床开发', '商业', '生产及供应链', '职能']
        for _, row in main_df.iterrows():
            fname = str(row.get('职能', ''))
            if any(kw in fname for kw in func_keywords) and '公司整体' not in fname:
                std_func = self._standardize_function(fname)
                total = row.get('招聘总量', 0) or 0
                if total > 0:
                    self.flat_records.append({
                        '公司': company, '规模': scale, '层级': '一级职能',
                        '职能': std_func, '职级': '整体',
                        '招聘总量': total,
                        'FTE新增': row.get('FTE新增'), 'FTE替换': row.get('FTE替换'),
                        'HR直招': row.get('HR直招'), '猎头_人': row.get('猎头_人'),
                        'RPO_人': row.get('RPO_人'), '内推_人': row.get('内推_人'),
                        '主动投递': row.get('主动投递'), '校招': row.get('校招'),
                        '内部转岗': row.get('内部转岗'),
                        '招聘周期_天': row.get('招聘周期_天'),
                        '外部渠道成本_万': row.get('外部渠道成本_万'),
                        '猎头费_万': row.get('猎头费_万'),
                        '内推费_万': row.get('内推费_万'),
                        'RPO费_万': row.get('RPO费_万'),
                    })

        # 职级行
        for _, row in main_df.iterrows():
            fname = str(row.get('职能', ''))
            if any(kw in fname for kw in ['VP', 'Director', 'Mgr', 'General']):
                std_level = self._standardize_level(fname)
                total = row.get('招聘总量', 0) or 0
                if total > 0:
                    self.flat_records.append({
                        '公司': company, '规模': scale, '层级': '职级',
                        '职能': '公司整体', '职级': std_level,
                        '招聘总量': total,
                        'FTE新增': row.get('FTE新增'), 'FTE替换': row.get('FTE替换'),
                        'HR直招': row.get('HR直招'), '猎头_人': row.get('猎头_人'),
                        'RPO_人': row.get('RPO_人'), '内推_人': row.get('内推_人'),
                        '内部转岗': row.get('内部转岗'),
                        '招聘周期_天': row.get('招聘周期_天'),
                        '外部渠道成本_万': row.get('外部渠道成本_万'),
                        '猎头费_万': row.get('猎头费_万'),
                    })

        # 研发细分
        rd_df = raw_data.get('rd_detail')
        if rd_df is not None:
            for _, row in rd_df.iterrows():
                func = str(row.get('三级职能', ''))
                level = str(row.get('职级', '整体'))
                total = row.get('招聘总量', 0) or 0
                if func and func != 'nan' and total > 0:
                    self.flat_records.append({
                        '公司': company, '规模': scale, '层级': '研发细分',
                        '职能': func, '职级': level,
                        '招聘总量': total,
                        'FTE新增': row.get('FTE新增'), 'FTE替换': row.get('FTE替换'),
                        'HR直招': row.get('HR直招'), '猎头_人': row.get('猎头_人'),
                        '内推_人': row.get('内推_人'), '内部转岗': row.get('内部转岗'),
                        '招聘周期_天': row.get('招聘周期_天'),
                        '外部渠道成本_万': row.get('外部渠道成本_万'),
                        '猎头费_万': row.get('猎头费_万'),
                    })

        # 商业细分
        comm_df = raw_data.get('commercial_detail')
        if comm_df is not None:
            for _, row in comm_df.iterrows():
                func = str(row.get('三级职能', ''))
                level = str(row.get('职级', '整体'))
                total = row.get('招聘总量', 0) or 0
                if func and func != 'nan' and total > 0:
                    self.flat_records.append({
                        '公司': company, '规模': scale, '层级': '商业细分',
                        '职能': func, '职级': level,
                        '招聘总量': total,
                        'FTE新增': row.get('FTE新增'), 'FTE替换': row.get('FTE替换'),
                        'HR直招': row.get('HR直招'), '猎头_人': row.get('猎头_人'),
                        '内推_人': row.get('内推_人'), '内部转岗': row.get('内部转岗'),
                        '招聘周期_天': row.get('招聘周期_天'),
                        '外部渠道成本_万': row.get('外部渠道成本_万'),
                        '猎头费_万': row.get('猎头费_万'),
                    })

        # TA配置
        ta_df = raw_data.get('ta_config')
        if ta_df is not None:
            for _, row in ta_df.iterrows():
                func = str(row.get('职能', ''))
                if func and func != 'nan':
                    self.flat_records.append({
                        '公司': company, '规模': scale, '层级': 'TA配置',
                        '职能': func, '职级': '整体',
                        'TA_FTE': row.get('TA_FTE'),
                        'TA_第三方': row.get('TA_第三方'),
                    })

    def _standardize_function(self, name):
        """标准化职能名称"""
        if '早期研发' in name or 'Discovery' in name:
            return '早期研发'
        elif '临床开发' in name or 'Clinical' in name:
            return '临床开发'
        elif '商业' in name or 'Commercial' in name:
            return '商业'
        elif '生产' in name or '供应链' in name or 'Manufacturing' in name:
            return '生产及供应链'
        elif '职能' in name or 'Enabling' in name:
            return '职能'
        return name

    def _standardize_level(self, name):
        """标准化职级名称"""
        if 'VP' in name:
            return 'VP and Above'
        elif 'Director' in name:
            return 'D-ED'
        elif 'Mgr' in name:
            return 'M-AD'
        elif 'General' in name:
            return 'General'
        return name

    def run_audit(self):
        """运行数据审核，Trim脱靶数据"""
        self.flat_records, self.audit_log = trim_outliers(self.flat_records)
        self.audit_report = generate_audit_report(self.audit_log)
        return self.audit_log

    def get_dataframe(self):
        """返回完整的扁平化DataFrame（已审核）"""
        if not hasattr(self, 'audit_log'):
            self.run_audit()
        df = pd.DataFrame(self.flat_records)
        # 确保必要列存在（即使为空）
        for col in ['层级', '公司', '规模', '职能', '职级', '招聘总量', '招聘周期_天',
                     '外部渠道成本_万', '猎头费_万', '内推费_万', 'RPO费_万',
                     'HR直招', '猎头_人', 'RPO_人', '内推_人', '主动投递', '校招', '内部转岗',
                     'FTE新增', 'FTE替换', 'TA_FTE', 'TA_第三方']:
            if col not in df.columns:
                df[col] = np.nan
        return df

    def get_summary(self):
        """返回汇总信息"""
        df = self.get_dataframe()
        return {
            '公司数': len(self.companies),
            '公司列表': list(self.companies.keys()),
            'A类公司': [n for n, d in self.companies.items()
                       if d.get('company_info', {}).get('公司规模分类') == 'A'],
            'B类公司': [n for n, d in self.companies.items()
                       if d.get('company_info', {}).get('公司规模分类') == 'B'],
            '总记录数': len(df),
        }


# ============================================================
# 3. 行业报告生成器
# ============================================================
class IndustryReportGenerator:
    """生成行业级TA效能分析报告"""

    def __init__(self, aggregator: IndustryAggregator):
        self.agg = aggregator
        self.df = aggregator.get_dataframe()
        self.summary = aggregator.get_summary()
        self._empty = (len(self.df) == 0 or '层级' not in self.df.columns)

    def _pct(self, series):
        """安全计算分位数"""
        s = pd.to_numeric(series, errors='coerce').dropna()
        if len(s) == 0:
            return {'P25': np.nan, 'P50': np.nan, 'P75': np.nan, '平均': np.nan, 'n': 0}
        return {
            'P25': np.percentile(s, 25),
            'P50': np.percentile(s, 50),
            'P75': np.percentile(s, 75),
            '平均': s.mean(),
            'n': len(s),
        }

    def _calc_ratio(self, df_sub, numerator_col, denominator_col):
        """计算比率的分位数"""
        ratios = []
        for _, row in df_sub.iterrows():
            num = pd.to_numeric(row.get(numerator_col), errors='coerce')
            den = pd.to_numeric(row.get(denominator_col), errors='coerce')
            if pd.notna(num) and pd.notna(den) and den > 0:
                ratios.append(num / den)
        return self._pct(pd.Series(ratios)) if ratios else self._pct(pd.Series(dtype=float))

    def generate_full_report(self) -> str:
        """生成完整行业报告（Markdown格式）"""
        lines = []
        lines.append("# 医疗健康行业 招聘效能数据项目报告")
        lines.append(f"\n生成时间: {datetime.datetime.now().strftime('%Y年%m月%d日')}")
        lines.append(f"\n参调公司: {self.summary['公司数']}家")
        lines.append(f"- A类公司(≥1500人): {len(self.summary['A类公司'])}家 — {', '.join(self.summary['A类公司'])}")
        lines.append(f"- B类公司(<1500人): {len(self.summary['B类公司'])}家 — {', '.join(self.summary['B类公司'])}")

        # Part 1: 整体概览
        lines.append("\n---\n## Part 1: 整体概览\n")
        lines.extend(self._section_overview())

        # Part 2: TA效能报告
        lines.append("\n---\n## Part 2: TA效能报告\n")
        lines.extend(self._section_recruitment_volume())
        lines.extend(self._section_channels())
        lines.extend(self._section_cost())
        lines.extend(self._section_time_to_hire())
        lines.extend(self._section_commercial_detail())
        lines.extend(self._section_rd_detail())
        lines.extend(self._section_ta_productivity())

        # Part 3: 附录 - 分位数表
        lines.append("\n---\n## 附录: 人力效益指标列表\n")
        lines.extend(self._appendix_volume())
        lines.extend(self._appendix_channels())
        lines.extend(self._appendix_time())
        lines.extend(self._appendix_cost())

        return "\n".join(lines)

    # ==================== 整体概览 ====================
    def _section_overview(self):
        lines = []
        overall = self.df[self.df['层级'] == '公司整体']
        if overall.empty:
            lines.append("*暂无公司整体数据*")
            return lines

        total_hire = overall['招聘总量'].sum()
        lines.append(f"### 行业招聘总量概览")
        lines.append(f"- 参调公司总招聘量: **{total_hire:.0f}人**")
        lines.append(f"- 公司平均招聘量: **{overall['招聘总量'].mean():.0f}人**")
        lines.append(f"- 招聘量P50: **{overall['招聘总量'].median():.0f}人**")

        # 各职能占比
        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        if not func_df.empty:
            lines.append("\n### 招聘量分布（按业务链条统计）")
            lines.append("| 职能 | 招聘量占比P50 |")
            lines.append("|------|-------------|")
            for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                func_data = func_df[func_df['职能'] == func]
                if not func_data.empty:
                    # 计算每家公司该职能占比
                    ratios = []
                    for _, row in func_data.iterrows():
                        co = row['公司']
                        co_total = overall[overall['公司'] == co]['招聘总量'].values
                        if len(co_total) > 0 and co_total[0] > 0:
                            ratios.append(row['招聘总量'] / co_total[0])
                    if ratios:
                        p50 = np.median(ratios)
                        lines.append(f"| {func} | {p50:.2%} |")

        return lines

    # ==================== 招聘量指标 ====================
    def _section_recruitment_volume(self):
        lines = []
        lines.append("### 各职能招聘量占比\n")

        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        overall = self.df[self.df['层级'] == '公司整体']

        if func_df.empty:
            return lines

        lines.append("| 职能 | 整体P50 | A类P50 | B类P50 |")
        lines.append("|------|---------|--------|--------|")

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            ratios_all, ratios_a, ratios_b = [], [], []
            func_data = func_df[func_df['职能'] == func]
            for _, row in func_data.iterrows():
                co = row['公司']
                co_total = overall[overall['公司'] == co]['招聘总量'].values
                if len(co_total) > 0 and co_total[0] > 0:
                    r = row['招聘总量'] / co_total[0]
                    ratios_all.append(r)
                    if row['规模'] == 'A':
                        ratios_a.append(r)
                    else:
                        ratios_b.append(r)

            p50_all = f"{np.median(ratios_all):.2%}" if ratios_all else "N/A"
            p50_a = f"{np.median(ratios_a):.2%}" if ratios_a else "N/A"
            p50_b = f"{np.median(ratios_b):.2%}" if ratios_b else "N/A"
            lines.append(f"| {func} | {p50_all} | {p50_a} | {p50_b} |")

        return lines

    # ==================== 招聘渠道 ====================
    def _section_channels(self):
        lines = []
        lines.append("\n### 招聘渠道分布\n")

        overall = self.df[self.df['层级'] == '公司整体']
        if overall.empty:
            return lines

        # 计算各渠道占比
        def _safe(val):
            """NaN-safe: np.nan or 0 返回 nan 而非 0, 这里修正"""
            try:
                v = float(val)
                return v if pd.notna(v) else 0.0
            except (TypeError, ValueError):
                return 0.0

        channel_data = []
        for _, row in overall.iterrows():
            total = _safe(row.get('招聘总量', 0))
            if total <= 0:
                continue
            hr = _safe(row.get('HR直招', 0)) / total
            hh = _safe(row.get('猎头_人', 0)) / total
            ref = _safe(row.get('内推_人', 0)) / total
            transfer = _safe(row.get('内部转岗', 0)) / total
            rpo = _safe(row.get('RPO_人', 0)) / total
            apply_direct = _safe(row.get('主动投递', 0)) / total
            campus = _safe(row.get('校招', 0)) / total
            ext = hh + rpo + ref + apply_direct + campus
            channel_data.append({
                '公司': row['公司'], '规模': row['规模'],
                'HR直招': hr, '外部渠道': ext, '内部渠道': transfer,
                '猎头': hh, '内推': ref,
            })

        ch_df = pd.DataFrame(channel_data)
        if ch_df.empty:
            return lines

        def _fmt_pct(v):
            return f"{v:.2%}" if pd.notna(v) else "N/A"

        lines.append("#### 整体渠道分布 P50")
        lines.append("| 渠道 | 整体 | A类 | B类 |")
        lines.append("|------|------|-----|-----|")
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            p50_all = ch_df[ch].median()
            p50_a = ch_df[ch_df['规模'] == 'A'][ch].median() if len(ch_df[ch_df['规模'] == 'A']) > 0 else np.nan
            p50_b = ch_df[ch_df['规模'] == 'B'][ch].median() if len(ch_df[ch_df['规模'] == 'B']) > 0 else np.nan
            lines.append(f"| {ch} | {_fmt_pct(p50_all)} | {_fmt_pct(p50_a)} | {_fmt_pct(p50_b)} |")

        lines.append("\n#### 外部渠道细分 P50")
        lines.append("| 渠道 | 整体 | A类 | B类 |")
        lines.append("|------|------|-----|-----|")
        for ch in ['猎头', '内推']:
            p50_all = ch_df[ch].median()
            p50_a = ch_df[ch_df['规模'] == 'A'][ch].median() if len(ch_df[ch_df['规模'] == 'A']) > 0 else np.nan
            p50_b = ch_df[ch_df['规模'] == 'B'][ch].median() if len(ch_df[ch_df['规模'] == 'B']) > 0 else np.nan
            lines.append(f"| {ch} | {_fmt_pct(p50_all)} | {_fmt_pct(p50_a)} | {_fmt_pct(p50_b)} |")

        return lines

    # ==================== 招聘成本 ====================
    def _section_cost(self):
        lines = []
        lines.append("\n### 招聘成本分析\n")

        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        if func_df.empty:
            return lines

        lines.append("#### 渠道单个职位招聘成本 P50（万元）")
        lines.append("| 职能 | P50 |")
        lines.append("|------|-----|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            fd = func_df[func_df['职能'] == func]
            costs = []
            for _, row in fd.iterrows():
                cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
                total = pd.to_numeric(row.get('招聘总量'), errors='coerce')
                if pd.notna(cost) and pd.notna(total) and total > 0:
                    costs.append(cost / total)
            if costs:
                lines.append(f"| {func} | {np.median(costs):.2f} |")

        # 成本结构
        lines.append("\n#### 招聘渠道成本分布 P50")
        overall = self.df[self.df['层级'] == '公司整体']
        cost_data = []
        for _, row in overall.iterrows():
            total_cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            hh_cost = pd.to_numeric(row.get('猎头费_万'), errors='coerce')
            ref_cost = pd.to_numeric(row.get('内推费_万'), errors='coerce')
            if pd.notna(total_cost) and total_cost > 0:
                cost_data.append({
                    '规模': row['规模'],
                    '猎头费占比': hh_cost / total_cost if pd.notna(hh_cost) else np.nan,
                    '内推费占比': ref_cost / total_cost if pd.notna(ref_cost) else np.nan,
                })
        if cost_data:
            cdf = pd.DataFrame(cost_data)
            lines.append("| 渠道 | 整体 | A类 | B类 |")
            lines.append("|------|------|-----|-----|")
            for col in ['猎头费占比', '内推费占比']:
                label = col.replace('占比', '')
                p50_all = cdf[col].median()
                p50_a = cdf[cdf['规模'] == 'A'][col].median() if len(cdf[cdf['规模'] == 'A']) > 0 else np.nan
                p50_b = cdf[cdf['规模'] == 'B'][col].median() if len(cdf[cdf['规模'] == 'B']) > 0 else np.nan
                fmt = lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
                lines.append(f"| {label} | {fmt(p50_all)} | {fmt(p50_a)} | {fmt(p50_b)} |")

        return lines

    # ==================== 招聘周期 ====================
    def _section_time_to_hire(self):
        lines = []
        lines.append("\n### 招聘周期分析\n")

        # 各职能
        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        lines.append("#### 各职能招聘周期 P50（天）")
        lines.append("| 职能 | 整体P50 | A类P50 | B类P50 |")
        lines.append("|------|---------|--------|--------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            fd = func_df[func_df['职能'] == func]
            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
            tth_a = pd.to_numeric(fd[fd['规模'] == 'A']['招聘周期_天'], errors='coerce').dropna()
            tth_b = pd.to_numeric(fd[fd['规模'] == 'B']['招聘周期_天'], errors='coerce').dropna()
            fmt = lambda s: f"{s.median():.1f}" if len(s) > 0 else "N/A"
            lines.append(f"| {func} | {fmt(tth)} | {fmt(tth_a)} | {fmt(tth_b)} |")

        # 各职级
        level_df = self.df[self.df['层级'] == '职级']
        if not level_df.empty:
            lines.append("\n#### 各职级招聘周期 P50（天）")
            lines.append("| 职级 | P50 |")
            lines.append("|------|-----|")
            for level in ['VP and Above', 'D-ED', 'M-AD', 'General']:
                ld = level_df[level_df['职级'] == level]
                tth = pd.to_numeric(ld['招聘周期_天'], errors='coerce').dropna()
                if len(tth) > 0:
                    lines.append(f"| {level} | {tth.median():.1f} |")

        return lines

    # ==================== 商业细分 ====================
    def _section_commercial_detail(self):
        lines = []
        comm_df = self.df[(self.df['层级'] == '商业细分') & (self.df['职级'] == '整体')]
        if comm_df.empty:
            return lines

        lines.append("\n### 商业职能细分数据\n")
        lines.append("#### 各二级职能招聘周期 P50（天）")
        funcs = comm_df['职能'].unique()
        lines.append("| 二级职能 | P50 | 样本数 |")
        lines.append("|----------|-----|--------|")
        for f in sorted(funcs):
            fd = comm_df[comm_df['职能'] == f]
            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
            if len(tth) > 0:
                lines.append(f"| {f} | {tth.median():.1f} | {len(tth)} |")

        return lines

    # ==================== 研发细分 ====================
    def _section_rd_detail(self):
        lines = []
        rd_df = self.df[(self.df['层级'] == '研发细分') & (self.df['职级'] == '整体')]
        if rd_df.empty:
            return lines

        lines.append("\n### 研发职能细分数据\n")
        lines.append("#### 各二级职能招聘周期 P50（天）")
        funcs = rd_df['职能'].unique()
        lines.append("| 二级职能 | P50 | 样本数 |")
        lines.append("|----------|-----|--------|")
        for f in sorted(funcs):
            fd = rd_df[rd_df['职能'] == f]
            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
            if len(tth) > 0:
                lines.append(f"| {f} | {tth.median():.1f} | {len(tth)} |")

        return lines

    # ==================== TA生产率 ====================
    def _section_ta_productivity(self):
        lines = []
        ta_df = self.df[self.df['层级'] == 'TA配置']
        overall = self.df[self.df['层级'] == '公司整体']
        if ta_df.empty or overall.empty:
            return lines

        lines.append("\n### TA生产率\n")
        prod_data = []
        for co in self.agg.companies:
            co_ta = ta_df[(ta_df['公司'] == co) & (ta_df['职能'].str.contains('公司整体', na=False))]
            co_hire = overall[overall['公司'] == co]
            if not co_ta.empty and not co_hire.empty:
                ta_fte = pd.to_numeric(co_ta.iloc[0].get('TA_FTE'), errors='coerce')
                hire = co_hire.iloc[0].get('招聘总量', 0)
                if pd.notna(ta_fte) and ta_fte > 0 and hire > 0:
                    prod_data.append({
                        '公司': co, '规模': co_hire.iloc[0]['规模'],
                        'TA_FTE': ta_fte, '招聘总量': hire,
                        '人均招聘量': hire / ta_fte,
                    })

        if prod_data:
            pdf = pd.DataFrame(prod_data)
            lines.append("| 指标 | 整体P50 | A类P50 | B类P50 |")
            lines.append("|------|---------|--------|--------|")
            p50_all = pdf['人均招聘量'].median()
            p50_a = pdf[pdf['规模'] == 'A']['人均招聘量'].median() if len(pdf[pdf['规模'] == 'A']) > 0 else np.nan
            p50_b = pdf[pdf['规模'] == 'B']['人均招聘量'].median() if len(pdf[pdf['规模'] == 'B']) > 0 else np.nan
            fmt = lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
            lines.append(f"| 人均招聘量 | {fmt(p50_all)} | {fmt(p50_a)} | {fmt(p50_b)} |")

        return lines

    # ==================== 附录 ====================
    def _appendix_volume(self):
        lines = ["### 附录1: 招聘量指标\n"]
        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        overall = self.df[self.df['层级'] == '公司整体']

        lines.append("#### 各职能招聘量占比")
        lines.append("| 职能 | n | P25 | P50 | P75 | 平均 |")
        lines.append("|------|---|-----|-----|-----|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            ratios = []
            fd = func_df[func_df['职能'] == func]
            for _, row in fd.iterrows():
                co_total = overall[overall['公司'] == row['公司']]['招聘总量'].values
                if len(co_total) > 0 and co_total[0] > 0:
                    ratios.append(row['招聘总量'] / co_total[0])
            p = self._pct(pd.Series(ratios))
            fmt = lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
            n_note = f"⚠️{p['n']}" if p['n'] <= 2 else str(p['n'])
            lines.append(f"| {func} | {n_note} | {fmt(p['P25'])} | {fmt(p['P50'])} | {fmt(p['P75'])} | {fmt(p['平均'])} |")

        if any(p['n'] <= 2 for func in ['早期研发'] for p in [self._pct(pd.Series([1]))]):
            pass  # note below covers it
        lines.append("\n> ⚠️ 标注表示样本量≤2，分位数参考性有限，建议增加更多公司数据\n")

        return lines

    def _appendix_channels(self):
        lines = ["\n### 附录2: 招聘渠道指标\n"]
        overall = self.df[self.df['层级'] == '公司整体']

        def _safe(val):
            try:
                v = float(val)
                return v if pd.notna(v) else 0.0
            except (TypeError, ValueError):
                return 0.0

        ch_ratios = defaultdict(list)
        for _, row in overall.iterrows():
            total = _safe(row.get('招聘总量', 0))
            if total <= 0:
                continue
            ch_ratios['HR直招'].append(_safe(row.get('HR直招', 0)) / total)
            ch_ratios['猎头'].append(_safe(row.get('猎头_人', 0)) / total)
            ch_ratios['内推'].append(_safe(row.get('内推_人', 0)) / total)
            ch_ratios['内部转岗'].append(_safe(row.get('内部转岗', 0)) / total)

        lines.append("#### 各招聘渠道招聘量占比")
        lines.append("| 渠道 | n | P25 | P50 | P75 | 平均 |")
        lines.append("|------|---|-----|-----|-----|------|")
        for ch in ['HR直招', '猎头', '内推', '内部转岗']:
            p = self._pct(pd.Series(ch_ratios[ch]))
            fmt = lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
            n_note = f"⚠️{p['n']}" if p['n'] <= 2 else str(p['n'])
            lines.append(f"| {ch} | {n_note} | {fmt(p['P25'])} | {fmt(p['P50'])} | {fmt(p['P75'])} | {fmt(p['平均'])} |")

        return lines

    def _appendix_time(self):
        lines = ["\n### 附录3: 招聘周期指标\n"]

        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        lines.append("#### 各职能招聘周期（天）")
        lines.append("| 职能 | n | P25 | P50 | P75 | 平均 |")
        lines.append("|------|---|-----|-----|-----|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            fd = func_df[func_df['职能'] == func]
            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
            p = self._pct(tth)
            fmt = lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
            n_note = f"⚠️{p['n']}" if p['n'] <= 2 else str(p['n'])
            lines.append(f"| {func} | {n_note} | {fmt(p['P25'])} | {fmt(p['P50'])} | {fmt(p['P75'])} | {fmt(p['平均'])} |")

        level_df = self.df[self.df['层级'] == '职级']
        if not level_df.empty:
            lines.append("\n#### 各职级招聘周期（天）")
            lines.append("| 职级 | n | P25 | P50 | P75 | 平均 |")
            lines.append("|------|---|-----|-----|-----|------|")
            for level in ['VP and Above', 'D-ED', 'M-AD', 'General']:
                ld = level_df[level_df['职级'] == level]
                tth = pd.to_numeric(ld['招聘周期_天'], errors='coerce').dropna()
                p = self._pct(tth)
                fmt = lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
                n_note = f"⚠️{p['n']}" if p['n'] <= 2 else str(p['n'])
                lines.append(f"| {level} | {n_note} | {fmt(p['P25'])} | {fmt(p['P50'])} | {fmt(p['P75'])} | {fmt(p['平均'])} |")

        lines.append("\n> ⚠️ 标注表示样本量≤2，P25/P50/P75相同属正常现象，需更多公司数据才能体现分位差异\n")
        return lines

    def _appendix_cost(self):
        lines = ["\n### 附录4: 招聘成本指标\n"]

        func_df = self.df[(self.df['层级'] == '一级职能') & (self.df['职级'] == '整体')]
        lines.append("#### 不同职能人均招聘成本（万元）")
        lines.append("| 职能 | n | P25 | P50 | P75 | 平均 |")
        lines.append("|------|---|-----|-----|-----|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            fd = func_df[func_df['职能'] == func]
            costs = []
            for _, row in fd.iterrows():
                cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
                total = pd.to_numeric(row.get('招聘总量'), errors='coerce')
                if pd.notna(cost) and pd.notna(total) and total > 0:
                    costs.append(cost / total)
            p = self._pct(pd.Series(costs))
            fmt = lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            n_note = f"⚠️{p['n']}" if p['n'] <= 2 else str(p['n'])
            lines.append(f"| {func} | {n_note} | {fmt(p['P25'])} | {fmt(p['P50'])} | {fmt(p['P75'])} | {fmt(p['平均'])} |")

        return lines


# ============================================================
# 测试入口
# ============================================================
if __name__ == '__main__':
    # 测试：用百济神州单家数据
    filepath = r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源\2024TA  BeiGene_20250312.xlsx'

    agg = IndustryAggregator()
    raw = ingest_company(filepath)
    name = agg.add_company(raw)
    print(f"已添加: {name}")

    summary = agg.get_summary()
    print(f"汇总: {summary}")

    gen = IndustryReportGenerator(agg)
    report = gen.generate_full_report()

    output_path = r'c:\Users\EDY\.kimi\skills\ta_report_automation\llm_wiki\output_beigene\行业报告_测试.md'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n报告已保存: {output_path}")
    print(f"报告长度: {len(report)} 字符")
