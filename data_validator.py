"""
数据清洗与验证模块
===================
实现 Raw Data → 验证清洗 → Wiki 的数据流

基于《医药行业猎头TA数据核查总表》中的数据校准规则：
1. 剔除"无"导致的0值
2. 回溯错值（如单位不一致）
3. 舍弃样本量不足的指标
4. 统一单位用法
5. 各维度加总校验

参调公司清单（17家）：
A类(≥1500人): 辉致、辉瑞、科赴、诺华、卫材、信达、罗氏、百济神州
B类(<1500人): 吉利德、迪哲、默克、雅培、参天、赛诺菲、欧加隆、艾伯维、SMPC
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum


# ============================================================
# 公司注册表 - 来源于核查总表
# ============================================================
COMPANY_REGISTRY = {
    'Gilead': {'name': '吉利德', 'class': 'B', 'type': '研发+商业'},
    '迪哲': {'name': '迪哲', 'class': 'B', 'type': '纯研发'},
    'Merck': {'name': '默克', 'class': 'B', 'type': ''},
    '辉致': {'name': '辉致', 'class': 'A', 'type': '纯研发'},
    '雅培': {'name': '雅培', 'class': 'B', 'type': '无研发'},
    '辉瑞': {'name': '辉瑞', 'class': 'A', 'type': '研发+商业'},
    '参天': {'name': '参天', 'class': 'B', 'type': '研发+商业'},
    'Sanofi': {'name': '赛诺菲', 'class': 'B', 'type': '研发+商业'},
    '科赴': {'name': '科赴', 'class': 'A', 'type': '研发+商业'},
    '诺华': {'name': '诺华', 'class': 'A', 'type': '研发+商业'},
    '欧加隆': {'name': '欧加隆', 'class': 'B', 'type': '纯商业'},
    '卫材': {'name': '卫材', 'class': 'A', 'type': '无研发'},
    '信达': {'name': '信达', 'class': 'A', 'type': '研发+商业'},
    'ABV': {'name': '艾伯维', 'class': 'B', 'type': '研发+商业'},
    '罗氏': {'name': '罗氏', 'class': 'A', 'type': '研发+商业'},
    'SMPC': {'name': 'SMPC', 'class': 'B', 'type': '纯商业'},
    'BeiGene': {'name': '百济神州', 'class': 'A', 'type': '研发+商业'},
}

# 已知的单位问题（来源于核查总表信息缺漏表）
UNIT_CORRECTIONS = {
    '罗氏': {
        '成本单位': '元',  # 罗氏的成本数据单位是"元"而非"万元"
        '成本转换因子': 0.0001,  # 元 → 万元
    }
}


class Severity(Enum):
    """问题严重程度"""
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "❌"
    FIXED = "✅"


@dataclass
class ValidationIssue:
    """验证问题记录"""
    company: str
    sheet: str
    field: str
    dimension: str  # 职能/职级
    severity: Severity
    issue_type: str
    original_value: Optional[float] = None
    corrected_value: Optional[float] = None
    description: str = ""
    action: str = ""  # 处理方式

    def to_dict(self):
        return {
            '公司': self.company,
            '数据表': self.sheet,
            '字段': self.field,
            '维度': self.dimension,
            '严重程度': self.severity.value,
            '问题类型': self.issue_type,
            '原始值': self.original_value,
            '修正值': self.corrected_value,
            '描述': self.description,
            '处理方式': self.action,
        }


@dataclass
class ValidationResult:
    """验证结果"""
    company: str
    status: str  # 'pass', 'warning', 'fail', 'needs_correction'
    issues: List[ValidationIssue] = field(default_factory=list)
    cleaned_data: Optional[dict] = None

    @property
    def error_count(self):
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)

    @property
    def warning_count(self):
        return sum(1 for i in self.issues if i.severity == Severity.WARNING)

    @property
    def fixed_count(self):
        return sum(1 for i in self.issues if i.severity == Severity.FIXED)

    def summary(self):
        return (f"{self.company}: {self.status} | "
                f"❌{self.error_count} ⚠️{self.warning_count} ✅{self.fixed_count}")


# ============================================================
# 验证规则
# ============================================================
# 合理值范围 - 成本字段无上限（单位为万元），仅检查非负
VALID_RANGES = {
    '招聘总量':        (0, 50000),
    'FTE新增':         (0, 50000),
    'FTE替换':         (0, 50000),
    '招聘周期_天':     (1, 365),
    '外部渠道成本_万': (0, float('inf')),   # 无上限
    '猎头费_万':       (0, float('inf')),   # 无上限
    '内推费_万':       (0, float('inf')),   # 无上限
    'RPO费_万':        (0, float('inf')),   # 无上限
    'HR直招':          (0, 50000),
    '猎头_人':         (0, 50000),
    'RPO_人':          (0, 50000),
    '内推_人':         (0, 50000),
    '主动投递':        (0, 50000),
    '校招':            (0, 50000),
    '内部转岗':        (0, 50000),
    'TA_FTE':          (0, 500),
    'TA_第三方':       (0, 500),
}


class DataValidator:
    """数据验证器 - 实现核查总表中的清洗逻辑"""

    def __init__(self):
        self.issues: List[ValidationIssue] = []

    def validate_company(self, raw_data: dict) -> ValidationResult:
        """验证单家公司数据"""
        self.issues = []
        company = raw_data.get('company_info', {}).get('公司名称', '未知')

        # 1. 单位校正（如罗氏的元→万元）
        self._check_unit_correction(raw_data, company)

        # 2. 数值范围校验
        self._check_value_ranges(raw_data, company)

        # 3. 加总一致性校验
        self._check_sum_consistency(raw_data, company)

        # 4. 逻辑关系校验
        self._check_logical_consistency(raw_data, company)

        # 5. 零值/缺失处理
        self._check_zero_missing(raw_data, company)

        # 确定状态
        if any(i.severity == Severity.ERROR for i in self.issues):
            status = 'needs_correction'
        elif any(i.severity == Severity.WARNING for i in self.issues):
            status = 'warning'
        else:
            status = 'pass'

        return ValidationResult(
            company=company,
            status=status,
            issues=list(self.issues),
            cleaned_data=raw_data,
        )

    def _check_unit_correction(self, raw_data, company):
        """检查并修正单位问题"""
        correction = None
        for key, info in UNIT_CORRECTIONS.items():
            if key in company:
                correction = info
                break

        if not correction:
            return

        factor = correction.get('成本转换因子', 1)
        unit = correction.get('成本单位', '万元')
        cost_fields = ['外部渠道成本_万', '猎头费_万', '内推费_万', 'RPO费_万']

        for df_key in ['main_efficiency', 'rd_detail', 'commercial_detail']:
            df = raw_data.get(df_key)
            if df is None:
                continue
            for col in cost_fields:
                if col in df.columns:
                    mask = pd.to_numeric(df[col], errors='coerce').notna()
                    if mask.any():
                        old_vals = df.loc[mask, col].copy()
                        df.loc[mask, col] = pd.to_numeric(df.loc[mask, col], errors='coerce') * factor
                        for idx in df.loc[mask].index:
                            old_v = pd.to_numeric(old_vals.get(idx), errors='coerce')
                            new_v = pd.to_numeric(df.at[idx, col], errors='coerce')
                            if pd.notna(old_v) and old_v > 0:
                                self.issues.append(ValidationIssue(
                                    company=company, sheet=df_key, field=col,
                                    dimension=str(df.at[idx, '职能'] if '职能' in df.columns else ''),
                                    severity=Severity.FIXED,
                                    issue_type='单位修正',
                                    original_value=old_v,
                                    corrected_value=new_v,
                                    description=f'成本单位从{unit}转换为万元(×{factor})',
                                    action='自动修正',
                                ))

    def _check_value_ranges(self, raw_data, company):
        """检查数值范围"""
        for df_key in ['main_efficiency', 'rd_detail', 'commercial_detail']:
            df = raw_data.get(df_key)
            if df is None:
                continue
            for col, (lo, hi) in VALID_RANGES.items():
                if col not in df.columns:
                    continue
                vals = pd.to_numeric(df[col], errors='coerce')
                for idx, val in vals.items():
                    if pd.isna(val):
                        continue
                    dim = str(df.at[idx, '职能'] if '职能' in df.columns else '')
                    if val < lo:
                        self.issues.append(ValidationIssue(
                            company=company, sheet=df_key, field=col,
                            dimension=dim, severity=Severity.FIXED,
                            issue_type='低于下限',
                            original_value=val,
                            corrected_value=np.nan,
                            description=f'值{val}低于下限{lo}，自动排除',
                            action='自动置为NaN',
                        ))
                        df.at[idx, col] = np.nan
                    elif hi != float('inf') and val > hi:
                        self.issues.append(ValidationIssue(
                            company=company, sheet=df_key, field=col,
                            dimension=dim, severity=Severity.FIXED,
                            issue_type='超过上限',
                            original_value=val,
                            corrected_value=np.nan,
                            description=f'值{val}超过上限{hi}，自动排除',
                            action='自动置为NaN',
                        ))
                        df.at[idx, col] = np.nan

    def _check_sum_consistency(self, raw_data, company):
        """检查加总一致性"""
        df = raw_data.get('main_efficiency')
        if df is None:
            return

        # 检查公司整体 vs 各职能加总
        overall = df[df['职能'].astype(str).str.contains('公司整体', na=False)]
        funcs = df[df['职能'].astype(str).str.contains('早期研发|临床开发|商业|生产及供应链|职能', na=False)]
        funcs = funcs[~funcs['职能'].astype(str).str.contains('公司整体', na=False)]

        if not overall.empty and not funcs.empty:
            for col in ['招聘总量', 'HR直招', '猎头_人', '内推_人', '内部转岗']:
                if col not in df.columns:
                    continue
                total = pd.to_numeric(overall.iloc[0].get(col), errors='coerce')
                parts_sum = pd.to_numeric(funcs[col], errors='coerce').sum()
                if pd.notna(total) and total > 0 and pd.notna(parts_sum) and parts_sum > 0:
                    diff_pct = abs(total - parts_sum) / total
                    if diff_pct > 0.05:  # >5%差异
                        self.issues.append(ValidationIssue(
                            company=company, sheet='main_efficiency', field=col,
                            dimension='公司整体 vs 各职能',
                            severity=Severity.WARNING,
                            issue_type='加总不一致',
                            original_value=total,
                            corrected_value=parts_sum,
                            description=f'公司整体{col}={total:.0f}，各职能加总={parts_sum:.0f}，差异{diff_pct:.1%}',
                            action='保留原值，标记警告',
                        ))

        # 检查渠道加总 vs 招聘总量
        for _, row in df.iterrows():
            dim = str(row.get('职能', ''))
            total = pd.to_numeric(row.get('招聘总量'), errors='coerce')
            if pd.isna(total) or total <= 0:
                continue
            channel_cols = ['HR直招', '猎头_人', 'RPO_人', '内推_人', '主动投递', '校招', '内部转岗']
            ch_sum = sum(pd.to_numeric(row.get(c, 0) or 0, errors='coerce') or 0 for c in channel_cols if c in df.columns)
            if ch_sum > 0:
                diff_pct = abs(total - ch_sum) / total
                if diff_pct > 0.1:  # >10%差异
                    self.issues.append(ValidationIssue(
                        company=company, sheet='main_efficiency', field='渠道加总',
                        dimension=dim,
                        severity=Severity.WARNING,
                        issue_type='渠道加总不一致',
                        original_value=total,
                        corrected_value=ch_sum,
                        description=f'{dim}招聘总量={total:.0f}，渠道加总={ch_sum:.0f}，差异{diff_pct:.1%}',
                        action='保留原值，标记警告',
                    ))

    def _check_logical_consistency(self, raw_data, company):
        """检查逻辑一致性"""
        df = raw_data.get('main_efficiency')
        if df is None:
            return

        for _, row in df.iterrows():
            dim = str(row.get('职能', ''))
            total = pd.to_numeric(row.get('招聘总量'), errors='coerce')

            # 成本应与招聘量正相关
            cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            if pd.notna(cost) and pd.notna(total) and total > 0 and cost > 0:
                per_hire = cost / total
                if per_hire > 50:  # 单个职位成本>50万，可能异常
                    self.issues.append(ValidationIssue(
                        company=company, sheet='main_efficiency', field='人均成本',
                        dimension=dim, severity=Severity.WARNING,
                        issue_type='人均成本异常高',
                        original_value=per_hire,
                        description=f'{dim}人均招聘成本{per_hire:.1f}万/人，偏高',
                        action='保留，标记关注',
                    ))

    def _check_zero_missing(self, raw_data, company):
        """处理零值和缺失"""
        df = raw_data.get('main_efficiency')
        if df is None:
            return

        for _, row in df.iterrows():
            dim = str(row.get('职能', ''))
            # 招聘周期为0但有招聘量 → 剔除0
            tth = pd.to_numeric(row.get('招聘周期_天'), errors='coerce')
            total = pd.to_numeric(row.get('招聘总量'), errors='coerce')
            if tth == 0 and pd.notna(total) and total > 0:
                self.issues.append(ValidationIssue(
                    company=company, sheet='main_efficiency', field='招聘周期_天',
                    dimension=dim, severity=Severity.FIXED,
                    issue_type='零值剔除',
                    original_value=0,
                    description=f'{dim}有招聘量{total:.0f}但招聘周期为0，剔除',
                    action='置为NaN',
                ))
                # 在DataFrame中修正
                idx = row.name
                df.at[idx, '招聘周期_天'] = np.nan


def generate_validation_report(results: List[ValidationResult]) -> str:
    """生成数据验证报告（Markdown）"""
    lines = [
        "# 📋 数据清洗与验证报告\n",
        f"验证公司数: **{len(results)}**\n",
    ]

    # 汇总
    pass_count = sum(1 for r in results if r.status == 'pass')
    warn_count = sum(1 for r in results if r.status == 'warning')
    fail_count = sum(1 for r in results if r.status == 'needs_correction')
    total_issues = sum(len(r.issues) for r in results)
    total_fixed = sum(r.fixed_count for r in results)

    lines.append("## 验证汇总\n")
    lines.append(f"| 状态 | 数量 |")
    lines.append(f"|------|------|")
    lines.append(f"| ✅ 通过 | {pass_count} |")
    lines.append(f"| ⚠️ 有警告 | {warn_count} |")
    lines.append(f"| ❌ 需修正 | {fail_count} |")
    lines.append(f"| 📊 问题总数 | {total_issues} |")
    lines.append(f"| 🔧 已自动修正 | {total_fixed} |")

    # 各公司详情
    lines.append("\n## 各公司验证详情\n")

    for r in sorted(results, key=lambda x: x.error_count, reverse=True):
        status_icon = {'pass': '✅', 'warning': '⚠️', 'needs_correction': '❌'}.get(r.status, '❓')
        lines.append(f"\n### {status_icon} {r.company}")
        lines.append(f"状态: {r.status} | ❌{r.error_count} ⚠️{r.warning_count} ✅{r.fixed_count}\n")

        if r.issues:
            lines.append("| 严重程度 | 问题类型 | 字段 | 维度 | 原始值 | 修正值 | 描述 | 处理 |")
            lines.append("|----------|----------|------|------|--------|--------|------|------|")
            for i in r.issues:
                orig = f"{i.original_value:.2f}" if i.original_value is not None else ""
                corr = f"{i.corrected_value:.2f}" if i.corrected_value is not None else ""
                lines.append(f"| {i.severity.value} | {i.issue_type} | {i.field} | {i.dimension} | {orig} | {corr} | {i.description} | {i.action} |")
        else:
            lines.append("*数据验证通过，无问题*")

    # 需客户修正的清单
    corrections_needed = [r for r in results if r.status == 'needs_correction']
    if corrections_needed:
        lines.append("\n## ⚠️ 需客户修正的数据\n")
        lines.append("以下数据需要返回客户确认后才能进入分析：\n")
        for r in corrections_needed:
            errors = [i for i in r.issues if i.severity == Severity.ERROR]
            lines.append(f"### {r.company}")
            for e in errors:
                lines.append(f"- **{e.field}** ({e.dimension}): {e.description}")

    return "\n".join(lines)


# ============================================================
# 集成到工作流
# ============================================================
def validate_and_clean(raw_data_list: List[dict]) -> Tuple[List[dict], List[ValidationResult], str]:
    """
    批量验证和清洗数据
    
    Args:
        raw_data_list: 各公司原始数据列表
    
    Returns:
        (cleaned_data_list, validation_results, report_markdown)
        - cleaned_data_list: 验证通过的清洗后数据（可进入Wiki）
        - validation_results: 各公司验证结果
        - report_markdown: 验证报告
    """
    validator = DataValidator()
    results = []
    cleaned = []

    for raw in raw_data_list:
        result = validator.validate_company(raw)
        results.append(result)

        # 只有pass和warning的数据进入Wiki
        if result.status in ('pass', 'warning'):
            cleaned.append(result.cleaned_data)

    report = generate_validation_report(results)
    return cleaned, results, report
