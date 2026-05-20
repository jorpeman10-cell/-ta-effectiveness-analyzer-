"""
Year-over-Year Comparator - 年度对比分析模块
v2.0 新功能：支持上传上一年度数据，生成两年趋势对比报告

比对维度:
  1. 招聘量指标（各职能占比、各职级占比）
  2. 招聘渠道（HR直招/外部渠道/内部渠道占比变化）
  3. 招聘周期（各职能/职级招聘周期变化）
  4. 招聘成本（人均成本变化、渠道成本结构变化）
  5. TA生产率变化
  6. 细分职能（商业/研发二级职能趋势）
"""
import datetime
import re
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from multi_company import IndustryAggregator, IndustryReportGenerator
from report_parser import PriorYearData


def _safe(val):
    """NaN-safe value extraction"""
    try:
        v = float(val)
        return v if pd.notna(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _positive_series(series):
    """Numeric series with NaN and zero/non-positive values removed."""
    s = pd.to_numeric(series, errors='coerce').dropna()
    return s[s > 0]


def _external_cost_hire_count(row) -> float:
    """Denominator for cost per hire: HH + referral + RPO + direct apply + campus hires."""
    return (
        _safe(row.get('猎头_人', 0))
        + _safe(row.get('内推_人', 0))
        + _safe(row.get('RPO_人', 0))
        + _safe(row.get('主动投递', 0))
        + _safe(row.get('校招', 0))
    )


def _fmt_pct(v):
    """Format as percentage"""
    return f"{v:.2%}" if pd.notna(v) and v != 0 else "N/A"


def _fmt_num(v, decimals=1):
    """Format number"""
    return f"{v:.{decimals}f}" if pd.notna(v) else "N/A"


def _fmt_pct_with_note(v, note=None):
    text = _fmt_pct(v)
    return f"{text}（{note}）" if note and text != "N/A" else text


def _df_to_markdown_table(df: pd.DataFrame) -> str:
    """Render a small DataFrame as markdown without optional tabulate dependency."""
    if df is None or df.empty:
        return ""
    cols = list(df.columns)

    def cell(value):
        if pd.isna(value):
            return ""
        text = str(value)
        return text.replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(cell(row[col]) for col in cols) + " |")
    return "\n".join(lines)


def _delta(curr, prev):
    """Calculate delta and format with arrow"""
    if pd.isna(curr) or pd.isna(prev) or prev == 0:
        return "N/A", ""
    diff = curr - prev
    pct_change = diff / abs(prev)
    if abs(pct_change) < 0.001:
        arrow = "→"
    elif diff > 0:
        arrow = "↑"
    else:
        arrow = "↓"
    return f"{pct_change:+.1%}", arrow


def _delta_pct(curr, prev):
    """Delta for percentage values (show pp change)"""
    if pd.isna(curr) or pd.isna(prev):
        return "N/A", ""
    diff_pp = (curr - prev) * 100  # percentage points
    if abs(diff_pp) < 0.1:
        arrow = "→"
    elif diff_pp > 0:
        arrow = "↑"
    else:
        arrow = "↓"
    return f"{diff_pp:+.1f}pp", arrow


class YoYComparator:
    """
    Year-over-Year 对比分析引擎
    
    输入: 两个年度的 IndustryAggregator
    输出: 对比趋势报告 (Markdown)
    """

    def __init__(self, curr_agg: IndustryAggregator, prev_agg: IndustryAggregator,
                 curr_year: str = "2024", prev_year: str = "2023"):
        self.curr_agg = curr_agg
        self.prev_agg = prev_agg
        self.curr_year = curr_year
        self.prev_year = prev_year

        # Build DataFrames
        self.curr_df = curr_agg.get_dataframe()
        self.prev_df = prev_agg.get_dataframe()

        self.curr_summary = curr_agg.get_summary()
        self.prev_summary = prev_agg.get_summary()

    def generate_yoy_report(self) -> str:
        """生成完整的年度对比趋势报告"""
        lines = []
        lines.append(f"# 医疗健康行业 TA效能 {self.prev_year} vs {self.curr_year} 年度对比报告")
        lines.append(f"\n生成时间: {datetime.datetime.now().strftime('%Y年%m月%d日')}")
        lines.append(f"\n---")

        # 参调概况对比
        lines.extend(self._section_participation())

        # 相同公司样本年度比较
        same_company_lines = self._section_same_company_yoy()
        if same_company_lines:
            lines.append(f"\n---\n## 相同公司样本年度比较\n")
            lines.extend(same_company_lines)

        # 1. 招聘量趋势
        lines.append(f"\n---\n## 1. 招聘量指标趋势\n")
        lines.extend(self._section_volume_yoy())

        # 2. 招聘渠道趋势
        lines.append(f"\n---\n## 2. 招聘渠道趋势\n")
        lines.extend(self._section_channel_yoy())

        # 3. 招聘周期趋势
        lines.append(f"\n---\n## 3. 招聘周期趋势\n")
        lines.extend(self._section_tth_yoy())

        # 4. 招聘成本趋势
        lines.append(f"\n---\n## 4. 招聘成本趋势\n")
        lines.extend(self._section_cost_yoy())

        # 5. TA生产率趋势
        lines.append(f"\n---\n## 5. TA生产率趋势\n")
        lines.extend(self._section_productivity_yoy())

        # 6. 细分职能趋势
        lines.append(f"\n---\n## 6. 细分职能趋势\n")
        lines.extend(self._section_detail_yoy())

        # 7. 关键发现摘要
        lines.append(f"\n---\n## 7. 年度趋势关键发现\n")
        lines.extend(self._section_key_findings())

        return "\n".join(lines)

    # ==================== 参调概况 ====================
    def _section_participation(self):
        lines = []
        lines.append(f"\n## 参调概况对比\n")
        lines.append(f"| 指标 | {self.prev_year} | {self.curr_year} | 变化 |")
        lines.append(f"|------|------|------|------|")

        prev_n = self.prev_summary['公司数']
        curr_n = self.curr_summary['公司数']
        lines.append(f"| 参调公司数 | {prev_n}家 | {curr_n}家 | {curr_n - prev_n:+d} |")

        prev_a = len(self.prev_summary['A类公司'])
        curr_a = len(self.curr_summary['A类公司'])
        lines.append(f"| A类公司 | {prev_a}家 | {curr_a}家 | {curr_a - prev_a:+d} |")

        prev_b = len(self.prev_summary['B类公司'])
        curr_b = len(self.curr_summary['B类公司'])
        lines.append(f"| B类公司 | {prev_b}家 | {curr_b}家 | {curr_b - prev_b:+d} |")

        # 总招聘量
        prev_overall = self.prev_df[self.prev_df['层级'] == '公司整体']
        curr_overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        prev_hires = _positive_series(prev_overall['招聘总量']) if not prev_overall.empty else pd.Series(dtype=float)
        curr_hires = _positive_series(curr_overall['招聘总量']) if not curr_overall.empty else pd.Series(dtype=float)
        prev_total = prev_hires.sum() if len(prev_hires) else 0
        curr_total = curr_hires.sum() if len(curr_hires) else 0
        delta, arrow = _delta(curr_total, prev_total)
        lines.append(f"| 总招聘量 | {prev_total:.0f}人 | {curr_total:.0f}人 | {arrow}{delta} |")

        prev_eff_n = len(prev_hires)
        curr_eff_n = len(curr_hires)
        lines.append(f"| 有效招聘量样本 | {prev_eff_n}家 | {curr_eff_n}家 | {curr_eff_n - prev_eff_n:+d} |")

        prev_avg = prev_hires.mean() if len(prev_hires) else 0
        curr_avg = curr_hires.mean() if len(curr_hires) else 0
        delta, arrow = _delta(curr_avg, prev_avg)
        lines.append(f"| 平均招聘量 | {prev_avg:.0f}人 | {curr_avg:.0f}人 | {arrow}{delta} |")

        return lines

    # ==================== 招聘量趋势 ====================
    def _section_volume_yoy(self):
        lines = []

        # 各职能招聘量占比对比
        lines.append(f"### 各职能招聘量占比 P50 对比\n")
        lines.append(f"| 职能 | {self.prev_year} P50 | {self.curr_year} P50 | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev_p50 = self._calc_func_volume_p50(self.prev_df, func)
            curr_p50 = self._calc_func_volume_p50(self.curr_df, func)
            delta, arrow = _delta_pct(curr_p50, prev_p50)
            lines.append(f"| {func} | {_fmt_pct(prev_p50)} | {_fmt_pct(curr_p50)} | {delta} | {arrow} |")

        # 按公司规模分组
        lines.append(f"\n### 各职能招聘量占比 P50 对比（A类公司）\n")
        lines.append(f"| 职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev_p50 = self._calc_func_volume_p50(self.prev_df, func, scale='A')
            curr_p50 = self._calc_func_volume_p50(self.curr_df, func, scale='A')
            delta, arrow = _delta_pct(curr_p50, prev_p50)
            lines.append(f"| {func} | {_fmt_pct(prev_p50)} | {_fmt_pct(curr_p50)} | {delta} | {arrow} |")

        lines.append(f"\n### 各职能招聘量占比 P50 对比（B类公司）\n")
        lines.append(f"| 职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev_p50 = self._calc_func_volume_p50(self.prev_df, func, scale='B')
            curr_p50 = self._calc_func_volume_p50(self.curr_df, func, scale='B')
            delta, arrow = _delta_pct(curr_p50, prev_p50)
            lines.append(f"| {func} | {_fmt_pct(prev_p50)} | {_fmt_pct(curr_p50)} | {delta} | {arrow} |")

        return lines

    # ==================== 招聘渠道趋势 ====================
    def _section_channel_yoy(self):
        lines = []

        prev_ch = self._calc_channel_distribution(self.prev_df)
        curr_ch = self._calc_channel_distribution(self.curr_df)

        lines.append(f"### 整体渠道分布 P50 对比\n")
        lines.append(
            "> 注：一级渠道是闭合构成，但下表的 P50 是分别对每个渠道的公司占比取中位数，"
            "三个 P50 可能来自不同公司，因此不要求相加为100%，也可能出现三个P50同向变化。"
            "如需判断结构是否整体迁移，请同时查看导出的 P25/P50/P75 核查表或同公司样本。"
            "\n"
        )
        lines.append(f"| 渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            prev_v = prev_ch.get(('整体', ch), np.nan)
            curr_v = curr_ch.get(('整体', ch), np.nan)
            delta, arrow = _delta_pct(curr_v, prev_v)
            lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

        # A类 vs B类 渠道变化
        lines.append(f"\n### 外部渠道细分 P50 对比（占外部渠道）\n")
        lines.append(f"| 外部渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for ch in ['猎头', '内推', '主动投递', '校招', 'RPO']:
            prev_v = prev_ch.get(('整体', ch), np.nan)
            curr_v = curr_ch.get(('整体', ch), np.nan)
            delta, arrow = _delta_pct(curr_v, prev_v)
            lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

        for scale_label, scale_code in [('A类公司', 'A'), ('B类公司', 'B')]:
            lines.append(f"\n### {scale_label}一级渠道分布 P50 对比\n")
            lines.append(f"| 渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for ch in ['HR直招', '外部渠道', '内部渠道']:
                prev_v = prev_ch.get((scale_code, ch), np.nan)
                curr_v = curr_ch.get((scale_code, ch), np.nan)
                delta, arrow = _delta_pct(curr_v, prev_v)
                lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

            lines.append(f"\n### {scale_label}外部渠道细分 P50 对比（占外部渠道，不含RPO拆分）\n")
            lines.append(f"| 外部渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for ch in ['猎头', '内推', '主动投递', '校招']:
                prev_v = prev_ch.get((scale_code, ch), np.nan)
                curr_v = curr_ch.get((scale_code, ch), np.nan)
                delta, arrow = _delta_pct(curr_v, prev_v)
                lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

        return lines

    # ==================== 招聘周期趋势 ====================
    def _section_tth_yoy(self):
        lines = []

        lines.append(f"### 各职能招聘周期 P50 对比（天）\n")
        lines.append(f"| 职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev_p50 = self._calc_tth_p50(self.prev_df, '一级职能', '职能', func)
            curr_p50 = self._calc_tth_p50(self.curr_df, '一级职能', '职能', func)
            delta, arrow = _delta(curr_p50, prev_p50)
            lines.append(f"| {func} | {_fmt_num(prev_p50)} | {_fmt_num(curr_p50)} | {delta} | {arrow} |")

        # 按公司规模
        for scale_label, scale_code in [('A类公司', 'A'), ('B类公司', 'B')]:
            lines.append(f"\n### {scale_label}各职能招聘周期 P50 对比（天）\n")
            lines.append(f"| 职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                prev_p50 = self._calc_tth_p50(self.prev_df, '一级职能', '职能', func, scale=scale_code)
                curr_p50 = self._calc_tth_p50(self.curr_df, '一级职能', '职能', func, scale=scale_code)
                delta, arrow = _delta(curr_p50, prev_p50)
                lines.append(f"| {func} | {_fmt_num(prev_p50)} | {_fmt_num(curr_p50)} | {delta} | {arrow} |")

        # 各职级
        lines.append(f"\n### 各职级招聘周期 P50 对比（天）\n")
        lines.append(f"| 职级 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for level in ['VP and Above', 'D-ED', 'M-AD', 'General']:
            prev_p50 = self._calc_tth_p50(self.prev_df, '职级', '职级', level)
            curr_p50 = self._calc_tth_p50(self.curr_df, '职级', '职级', level)
            delta, arrow = _delta(curr_p50, prev_p50)
            lines.append(f"| {level} | {_fmt_num(prev_p50)} | {_fmt_num(curr_p50)} | {delta} | {arrow} |")

        return lines

    # ==================== 招聘成本趋势 ====================
    def _section_cost_yoy(self):
        lines = []

        lines.append(f"### 各职能人均招聘成本 P50 对比（万元）\n")
        lines.append("> 口径：各职能外部渠道费用成本 / 各职能（猎头+内部推荐+RPO+主动投递）的招聘总数。\n")
        lines.append(f"| 职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev_p50 = self._calc_cost_per_hire_p50(self.prev_df, func)
            curr_p50 = self._calc_cost_per_hire_p50(self.curr_df, func)
            delta, arrow = _delta(curr_p50, prev_p50)
            lines.append(f"| {func} | {_fmt_num(prev_p50, 2)} | {_fmt_num(curr_p50, 2)} | {delta} | {arrow} |")

        # 渠道成本结构变化
        lines.append(f"\n### 渠道成本结构 P50 对比\n")
        prev_cost = self._calc_cost_structure(self.prev_df)
        curr_cost = self._calc_cost_structure(self.curr_df)
        lines.append(f"| 渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for ch in ['猎头费占比', '内推费占比']:
            prev_v = prev_cost.get(ch, np.nan)
            curr_v = curr_cost.get(ch, np.nan)
            delta, arrow = _delta_pct(curr_v, prev_v)
            lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

        return lines

    # ==================== TA生产率趋势 ====================
    def _section_productivity_yoy(self):
        lines = []

        prev_prod = self._calc_ta_productivity(self.prev_df, self.prev_agg)
        curr_prod = self._calc_ta_productivity(self.curr_df, self.curr_agg)

        lines.append(f"### TA人均招聘量 P50 对比\n")
        lines.append(f"| 分组 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for group in ['整体', 'A', 'B']:
            label = {'整体': '整体', 'A': 'A类', 'B': 'B类'}[group]
            prev_v = prev_prod.get(group, np.nan)
            curr_v = curr_prod.get(group, np.nan)
            delta, arrow = _delta(curr_v, prev_v)
            lines.append(f"| {label} | {_fmt_num(prev_v)} | {_fmt_num(curr_v)} | {delta} | {arrow} |")

        return lines

    # ==================== 细分职能趋势 ====================
    def _section_detail_yoy(self):
        lines = []

        # 商业细分
        prev_comm = self.prev_df[(self.prev_df['层级'] == '商业细分') & (self.prev_df['职级'] == '整体')]
        curr_comm = self.curr_df[(self.curr_df['层级'] == '商业细分') & (self.curr_df['职级'] == '整体')]

        if not prev_comm.empty or not curr_comm.empty:
            lines.append(f"### 商业二级职能招聘周期 P50 对比（天）\n")
            all_funcs = sorted(set(prev_comm['职能'].unique()) | set(curr_comm['职能'].unique()))
            lines.append(f"| 二级职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|----------|------|------|------|------|")
            for f in all_funcs:
                prev_tth = pd.to_numeric(prev_comm[prev_comm['职能'] == f]['招聘周期_天'], errors='coerce').dropna()
                curr_tth = pd.to_numeric(curr_comm[curr_comm['职能'] == f]['招聘周期_天'], errors='coerce').dropna()
                prev_p50 = prev_tth.median() if len(prev_tth) > 0 else np.nan
                curr_p50 = curr_tth.median() if len(curr_tth) > 0 else np.nan
                delta, arrow = _delta(curr_p50, prev_p50)
                lines.append(f"| {f} | {_fmt_num(prev_p50)} | {_fmt_num(curr_p50)} | {delta} | {arrow} |")

        # 研发细分
        prev_rd = self.prev_df[(self.prev_df['层级'] == '研发细分') & (self.prev_df['职级'] == '整体')]
        curr_rd = self.curr_df[(self.curr_df['层级'] == '研发细分') & (self.curr_df['职级'] == '整体')]

        if not prev_rd.empty or not curr_rd.empty:
            lines.append(f"\n### 研发二级职能招聘周期 P50 对比（天）\n")
            all_funcs = sorted(set(prev_rd['职能'].unique()) | set(curr_rd['职能'].unique()))
            lines.append(f"| 二级职能 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|----------|------|------|------|------|")
            for f in all_funcs:
                prev_tth = pd.to_numeric(prev_rd[prev_rd['职能'] == f]['招聘周期_天'], errors='coerce').dropna()
                curr_tth = pd.to_numeric(curr_rd[curr_rd['职能'] == f]['招聘周期_天'], errors='coerce').dropna()
                prev_p50 = prev_tth.median() if len(prev_tth) > 0 else np.nan
                curr_p50 = curr_tth.median() if len(curr_tth) > 0 else np.nan
                delta, arrow = _delta(curr_p50, prev_p50)
                lines.append(f"| {f} | {_fmt_num(prev_p50)} | {_fmt_num(curr_p50)} | {delta} | {arrow} |")

        return lines

    # ==================== 关键发现 ====================
    def _section_key_findings(self):
        lines = []
        findings = []

        # 1. 招聘量变化
        prev_overall = self.prev_df[self.prev_df['层级'] == '公司整体']
        curr_overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        if not prev_overall.empty and not curr_overall.empty:
            prev_avg = prev_overall['招聘总量'].mean()
            curr_avg = curr_overall['招聘总量'].mean()
            if prev_avg > 0:
                change = (curr_avg - prev_avg) / prev_avg
                if abs(change) > 0.1:
                    direction = "增长" if change > 0 else "下降"
                    findings.append(f"**招聘规模{direction}**: 平均招聘量从{prev_avg:.0f}人{direction}至{curr_avg:.0f}人 ({change:+.1%})")

        # 2. 渠道结构变化
        prev_ch = self._calc_channel_distribution(self.prev_df)
        curr_ch = self._calc_channel_distribution(self.curr_df)
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            prev_v = prev_ch.get(('整体', ch), np.nan)
            curr_v = curr_ch.get(('整体', ch), np.nan)
            if pd.notna(prev_v) and pd.notna(curr_v):
                diff_pp = (curr_v - prev_v) * 100
                if abs(diff_pp) > 3:
                    direction = "上升" if diff_pp > 0 else "下降"
                    findings.append(f"**{ch}占比{direction}**: 从{prev_v:.1%}{direction}至{curr_v:.1%} ({diff_pp:+.1f}pp)")

        # 3. 招聘周期变化
        for func in ['商业', '临床开发', '早期研发']:
            prev_tth = self._calc_tth_p50(self.prev_df, '一级职能', '职能', func)
            curr_tth = self._calc_tth_p50(self.curr_df, '一级职能', '职能', func)
            if pd.notna(prev_tth) and pd.notna(curr_tth) and prev_tth > 0:
                change = (curr_tth - prev_tth) / prev_tth
                if abs(change) > 0.1:
                    direction = "延长" if change > 0 else "缩短"
                    findings.append(f"**{func}招聘周期{direction}**: 从{prev_tth:.0f}天{direction}至{curr_tth:.0f}天 ({change:+.1%})")

        # 4. 成本变化
        for func in ['商业', '临床开发']:
            prev_cost = self._calc_cost_per_hire_p50(self.prev_df, func)
            curr_cost = self._calc_cost_per_hire_p50(self.curr_df, func)
            if pd.notna(prev_cost) and pd.notna(curr_cost) and prev_cost > 0:
                change = (curr_cost - prev_cost) / prev_cost
                if abs(change) > 0.15:
                    direction = "上升" if change > 0 else "下降"
                    findings.append(f"**{func}人均成本{direction}**: 从{prev_cost:.2f}万{direction}至{curr_cost:.2f}万 ({change:+.1%})")

        if findings:
            for i, f in enumerate(findings, 1):
                lines.append(f"{i}. {f}")
        else:
            lines.append("*两年数据对比未发现显著变化趋势（变化幅度<10%）*")

        return lines

    # ==================== 相同公司样本 ====================
    def _normalize_company_name(self, name: str) -> str:
        """Normalize company names across years/files for paired comparison."""
        text = str(name or "").strip().lower()
        if not text or text in {"未知", "未知公司", "nan", "none"}:
            return ""
        text = re.sub(r"20\d{2}", "", text)
        text = re.sub(r"ta|效能|问卷|调研|数据|中国|制药|医药|有限公司|股份|集团|投资|上海|北京|\s+", "", text)
        text = re.sub(r"[_\-.（）()【】\[\] ]+", "", text)
        aliases = {
            "abv": "abbvie",
            "艾伯维": "abbvie",
            "abbvie": "abbvie",
            "abbott": "abbott",
            "雅培": "abbott",
            "beone": "beone",
            "百济": "beone",
            "百济神州": "beone",
            "bms": "bms",
            "百时美施贵宝": "bms",
            "ge": "ge",
            "gilead": "gilead",
            "吉利德": "gilead",
            "merck": "merck",
            "默克": "merck",
            "msd": "msd",
            "默沙东": "msd",
            "mpcn": "mpcn",
            "丸红": "mpcn",
            "novartis": "novartis",
            "诺华": "novartis",
            "pfizer": "pfizer",
            "辉瑞": "pfizer",
            "santen": "santen",
            "参天": "santen",
            "sanofi": "sanofi",
            "赛诺菲": "sanofi",
            "sa": "sanofi",
            "viatris": "viatris",
            "晖致": "viatris",
            "eisai": "eisai",
            "卫材": "eisai",
            "organon": "organon",
            "欧加隆": "organon",
            "kenvue": "kenvue",
            "科赴": "kenvue",
            "dizal": "dizal",
            "迪哲": "dizal",
        }
        for key, value in aliases.items():
            if key in text:
                return value
        return text

    def _same_company_pairs(self) -> Dict[str, Tuple[str, str]]:
        # MSD/默沙东与 Merck/默克在本项目数据源中不能自动视为同一家公司；
        # 同公司样本比较中先排除 MSD，避免与 Merck 或默克相关数据误配。
        excluded_keys = {"msd"}
        curr_map = {}
        prev_map = {}
        for name in self.curr_summary.get('公司列表', []):
            key = self._normalize_company_name(name)
            if key and key not in excluded_keys:
                curr_map.setdefault(key, name)
        for name in self.prev_summary.get('公司列表', []):
            key = self._normalize_company_name(name)
            if key and key not in excluded_keys:
                prev_map.setdefault(key, name)
        common = sorted(set(curr_map) & set(prev_map))
        return {key: (curr_map[key], prev_map[key]) for key in common}

    def _same_company_frames(self):
        pairs = self._same_company_pairs()
        curr_names = [curr for curr, _ in pairs.values()]
        prev_names = [prev for _, prev in pairs.values()]
        curr_df = self.curr_df[self.curr_df['公司'].isin(curr_names)].copy()
        prev_df = self.prev_df[self.prev_df['公司'].isin(prev_names)].copy()
        return pairs, curr_df, prev_df

    def _section_same_company_yoy(self):
        pairs, curr_df, prev_df = self._same_company_frames()
        if not pairs:
            return []

        lines = []
        matched_names = [f"{prev} → {curr}" if prev != curr else curr for curr, prev in pairs.values()]
        lines.append(f"- 同公司匹配样本: **{len(pairs)}家**")
        lines.append(f"- 匹配公司: {', '.join(matched_names)}")
        lines.append("\n> 本节仅使用两年均上传且成功识别的公司，剔除只在单一年份出现的公司，用于观察同一批公司的真实年度变化。\n")

        table = self.export_same_company_table()
        if table.empty:
            return lines

        for module in table['模块'].unique():
            sub = table[table['模块'] == module].drop(columns=['模块'])
            lines.append(f"\n### {module}\n")
            lines.append(_df_to_markdown_table(sub))

        return lines

    def export_same_company_table(self) -> pd.DataFrame:
        """Export paired same-company YoY table for questionnaire mode."""
        pairs, curr_df, prev_df = self._same_company_frames()
        if not pairs:
            return pd.DataFrame()

        rows = []
        prev_overall = prev_df[prev_df['层级'] == '公司整体']
        curr_overall = curr_df[curr_df['层级'] == '公司整体']

        def add_num(module, dimension, prev, curr, decimals=1):
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': module, '维度': dimension,
                f'{self.prev_year}同公司': _fmt_num(prev, decimals),
                f'{self.curr_year}同公司': _fmt_num(curr, decimals),
                '变化': delta, '趋势': arrow,
            })

        def add_pct(module, dimension, prev, curr):
            delta, arrow = _delta_pct(curr, prev)
            rows.append({
                '模块': module, '维度': dimension,
                f'{self.prev_year}同公司': _fmt_pct(prev),
                f'{self.curr_year}同公司': _fmt_pct(curr),
                '变化': delta, '趋势': arrow,
            })

        prev_hires = _positive_series(prev_overall['招聘总量']) if not prev_overall.empty else pd.Series(dtype=float)
        curr_hires = _positive_series(curr_overall['招聘总量']) if not curr_overall.empty else pd.Series(dtype=float)
        add_num('同公司招聘量', '总招聘量', prev_hires.sum() if len(prev_hires) else np.nan, curr_hires.sum() if len(curr_hires) else np.nan, 0)
        add_num('同公司招聘量', '公司平均招聘量', prev_hires.mean() if len(prev_hires) else np.nan, curr_hires.mean() if len(curr_hires) else np.nan, 0)
        add_num('同公司招聘量', '公司招聘量P50', prev_hires.median() if len(prev_hires) else np.nan, curr_hires.median() if len(curr_hires) else np.nan, 1)

        prev_ch = self._calc_channel_distribution(prev_df)
        curr_ch = self._calc_channel_distribution(curr_df)
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            add_pct('同公司渠道分布', ch, prev_ch.get(('整体', ch), np.nan), curr_ch.get(('整体', ch), np.nan))
        for ch in ['猎头', '内推', '主动投递', '校招', 'RPO']:
            add_pct('同公司外部渠道细分（占外部渠道）', ch, prev_ch.get(('整体', ch), np.nan), curr_ch.get(('整体', ch), np.nan))
        prev_close = self._calc_channel_closure(prev_df)
        curr_close = self._calc_channel_closure(curr_df)
        add_pct('同公司渠道闭合核查', '三渠道合计占比P50', prev_close.get('p50', np.nan), curr_close.get('p50', np.nan))
        add_pct('同公司渠道闭合核查', '三渠道合计占比(加权)', prev_close.get('weighted', np.nan), curr_close.get('weighted', np.nan))

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            add_pct('同公司职能招聘量占比', func, self._calc_func_volume_p50(prev_df, func), self._calc_func_volume_p50(curr_df, func))

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            add_num('同公司招聘周期', func, self._calc_tth_p50(prev_df, '一级职能', '职能', func), self._calc_tth_p50(curr_df, '一级职能', '职能', func), 1)

        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            add_num('同公司人均成本', func, self._calc_cost_per_hire_p50(prev_df, func), self._calc_cost_per_hire_p50(curr_df, func), 2)

        prev_prod = self._calc_ta_productivity(prev_df, self.prev_agg)
        curr_prod = self._calc_ta_productivity(curr_df, self.curr_agg)
        for group in ['整体', 'A', 'B']:
            add_num('同公司TA生产率', group, prev_prod.get(group, np.nan), curr_prod.get(group, np.nan), 1)

        return pd.DataFrame(rows)

    # ==================== 计算辅助方法 ====================

    def _calc_func_volume_p50(self, df, func, scale=None):
        """计算某职能的招聘量占比P50"""
        func_df = df[(df['层级'] == '一级职能') & (df['职级'] == '整体')]
        overall = df[df['层级'] == '公司整体']
        if scale:
            func_df = func_df[func_df['规模'] == scale]
            overall = overall[overall['规模'] == scale]

        fd = func_df[func_df['职能'] == func]
        ratios = []
        for _, row in fd.iterrows():
            ct = overall[overall['公司'] == row['公司']]['招聘总量'].values
            if len(ct) > 0 and ct[0] > 0:
                ratios.append(row['招聘总量'] / ct[0])
        return np.median(ratios) if ratios else np.nan

    def _calc_channel_distribution(self, df) -> Dict[tuple, float]:
        """计算渠道分布P50，返回 {(scale_or_all, channel): p50}"""
        overall = df[df['层级'] == '公司整体']
        result = {}

        channel_data = []
        for _, row in overall.iterrows():
            hr_n = _safe(row.get('HR直招', 0))
            hh_n = _safe(row.get('猎头_人', 0))
            ref_n = _safe(row.get('内推_人', 0))
            transfer_n = _safe(row.get('内部转岗', 0))
            rpo_n = _safe(row.get('RPO_人', 0))
            apply_n = _safe(row.get('主动投递', 0))
            campus_n = _safe(row.get('校招', 0))
            ext_n = hh_n + rpo_n + ref_n + apply_n + campus_n
            total = hr_n + ext_n + transfer_n
            if total <= 0:
                continue
            hr = hr_n / total
            transfer = transfer_n / total
            ext = ext_n / total
            hh = hh_n / ext_n if ext_n > 0 else np.nan
            ref = ref_n / ext_n if ext_n > 0 else np.nan
            rpo = rpo_n / ext_n if ext_n > 0 else np.nan
            apply_d = apply_n / ext_n if ext_n > 0 else np.nan
            campus = campus_n / ext_n if ext_n > 0 else np.nan

            channel_data.append({
                '规模': row['规模'],
                'HR直招': hr, '外部渠道': ext, '内部渠道': transfer,
                '猎头': hh, '内推': ref, '主动投递': apply_d, '校招': campus, 'RPO': rpo,
            })

        if not channel_data:
            return result

        chdf = pd.DataFrame(channel_data)
        for ch in ['HR直招', '外部渠道', '内部渠道', '猎头', '内推', '主动投递', '校招', 'RPO']:
            result[('整体', ch)] = chdf[ch].median()
            if ch != 'RPO':
                for scale in ['A', 'B']:
                    sub = chdf[chdf['规模'] == scale]
                    result[(scale, ch)] = sub[ch].median() if len(sub) > 0 else np.nan

        return result

    def _calc_channel_closure(self, df) -> Dict[str, float]:
        """Check whether HR direct + external + internal channels close to total hires."""
        overall = df[df['层级'] == '公司整体']
        ratios = []
        total_hires = 0.0
        total_channels = 0.0

        for _, row in overall.iterrows():
            total = _safe(row.get('招聘总量', 0))
            if total <= 0:
                continue
            hr = _safe(row.get('HR直招', 0))
            hh = _safe(row.get('猎头_人', 0))
            ref = _safe(row.get('内推_人', 0))
            transfer = _safe(row.get('内部转岗', 0))
            rpo = _safe(row.get('RPO_人', 0))
            apply_d = _safe(row.get('主动投递', 0))
            campus = _safe(row.get('校招', 0))
            channel_sum = hr + hh + ref + transfer + rpo + apply_d + campus
            ratios.append(channel_sum / total)
            total_hires += total
            total_channels += channel_sum

        return {
            'p50': np.median(ratios) if ratios else np.nan,
            'weighted': total_channels / total_hires if total_hires > 0 else np.nan,
        }

    def _calc_tth_p50(self, df, layer, group_col, group_val, scale=None):
        """计算招聘周期P50"""
        sub = df[df['层级'] == layer]
        if scale:
            sub = sub[sub['规模'] == scale]
        sub = sub[sub[group_col] == group_val]
        tth = pd.to_numeric(sub['招聘周期_天'], errors='coerce').dropna()
        return tth.median() if len(tth) > 0 else np.nan

    def _calc_cost_per_hire_p50(self, df, func):
        """计算某职能人均招聘成本P50"""
        func_df = df[(df['层级'] == '一级职能') & (df['职级'] == '整体')]
        fd = func_df[func_df['职能'] == func]
        costs = []
        for _, row in fd.iterrows():
            cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            external_hires = _external_cost_hire_count(row)
            if pd.notna(cost) and cost > 0 and external_hires > 0:
                costs.append(cost / external_hires)
        return np.median(costs) if costs else np.nan

    def _calc_cost_structure(self, df) -> Dict[str, float]:
        """计算渠道成本结构P50"""
        overall = df[df['层级'] == '公司整体']
        result = {}
        ratios_hh = []
        ratios_ref = []
        for _, row in overall.iterrows():
            tc = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            hh = pd.to_numeric(row.get('猎头费_万'), errors='coerce')
            ref = pd.to_numeric(row.get('内推费_万'), errors='coerce')
            if pd.notna(tc) and tc > 0:
                if pd.notna(hh):
                    ratios_hh.append(hh / tc)
                if pd.notna(ref):
                    ratios_ref.append(ref / tc)
        result['猎头费占比'] = np.median(ratios_hh) if ratios_hh else np.nan
        result['内推费占比'] = np.median(ratios_ref) if ratios_ref else np.nan
        return result

    def _calc_ta_productivity(self, df, agg) -> Dict[str, float]:
        """计算TA生产率P50"""
        ta_df = df[df['层级'] == 'TA配置']
        overall = df[df['层级'] == '公司整体']
        result = {}
        prod_data = []

        for co in agg.companies:
            co_ta = ta_df[(ta_df['公司'] == co) & (ta_df['职能'].str.contains('公司整体', na=False))]
            co_hire = overall[overall['公司'] == co]
            if not co_ta.empty and not co_hire.empty:
                ta_fte = pd.to_numeric(co_ta.iloc[0].get('TA_FTE'), errors='coerce')
                hire = co_hire.iloc[0].get('招聘总量', 0)
                if pd.notna(ta_fte) and ta_fte > 0 and hire > 0:
                    prod_data.append({
                        '规模': co_hire.iloc[0]['规模'],
                        '人均招聘量': hire / ta_fte,
                    })

        if prod_data:
            pdf = pd.DataFrame(prod_data)
            result['整体'] = pdf['人均招聘量'].median()
            for scale in ['A', 'B']:
                sub = pdf[pdf['规模'] == scale]
                result[scale] = sub['人均招聘量'].median() if len(sub) > 0 else np.nan

        return result

    # ==================== 数据表导出 ====================
    def export_comparison_table(self) -> pd.DataFrame:
        """导出年度对比汇总表（用于Streamlit展示）"""
        rows = []

        # 招聘量占比
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev = self._calc_func_volume_p50(self.prev_df, func)
            curr = self._calc_func_volume_p50(self.curr_df, func)
            delta, arrow = _delta_pct(curr, prev)
            rows.append({
                '模块': '招聘量占比', '维度': func,
                f'{self.prev_year}': _fmt_pct(prev),
                f'{self.curr_year}': _fmt_pct(curr),
                '变化': delta, '趋势': arrow,
            })

        # 渠道
        prev_ch = self._calc_channel_distribution(self.prev_df)
        curr_ch = self._calc_channel_distribution(self.curr_df)
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            prev_v = prev_ch.get(('整体', ch), np.nan)
            curr_v = curr_ch.get(('整体', ch), np.nan)
            delta, arrow = _delta_pct(curr_v, prev_v)
            rows.append({
                '模块': '招聘渠道', '维度': ch,
                f'{self.prev_year}': _fmt_pct(prev_v),
                f'{self.curr_year}': _fmt_pct(curr_v),
                '变化': delta, '趋势': arrow,
            })

        # 招聘周期
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev = self._calc_tth_p50(self.prev_df, '一级职能', '职能', func)
            curr = self._calc_tth_p50(self.curr_df, '一级职能', '职能', func)
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': '招聘周期', '维度': func,
                f'{self.prev_year}': _fmt_num(prev),
                f'{self.curr_year}': _fmt_num(curr),
                '变化': delta, '趋势': arrow,
            })

        # 成本
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev = self._calc_cost_per_hire_p50(self.prev_df, func)
            curr = self._calc_cost_per_hire_p50(self.curr_df, func)
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': '人均成本', '维度': func,
                f'{self.prev_year}': _fmt_num(prev, 2),
                f'{self.curr_year}': _fmt_num(curr, 2),
                '变化': delta, '趋势': arrow,
            })

        return pd.DataFrame(rows)


class YoYReportComparator:
    """
    Year-over-Year 对比引擎 (报告模式)
    
    对比方式: 当年调研数据 vs 上年度发布报告中的P50数据
    这样可以确保上年度数据与发布报告一致，避免原始调研数据被trim后的偏差
    
    输入:
      - curr_agg: 当年 IndustryAggregator (从问卷解析)
      - prev_data: 上年度 PriorYearData (从发布报告PDF解析)
    """

    def __init__(self, curr_agg: IndustryAggregator, prev_data: PriorYearData,
                 curr_year: str = "2025", prev_year: str = "2024",
                 survey_trend_report: str = ""):
        self.curr_agg = curr_agg
        self.prev_data = prev_data
        self.curr_year = curr_year
        self.prev_year = prev_year
        self.survey_sections = survey_trend_report if isinstance(survey_trend_report, dict) else {}
        self.survey_trend_report = survey_trend_report if isinstance(survey_trend_report, str) else ""

        self.curr_df = curr_agg.get_dataframe()
        self.curr_summary = curr_agg.get_summary()

    def generate_yoy_report(self) -> str:
        """生成年度对比报告 (当年调研 vs 上年度发布报告)"""
        lines = []
        lines.append(f"# 医疗健康行业 TA效能 {self.prev_year} vs {self.curr_year} 年度对比报告")
        prev_label = "最终口径表" if str(self.prev_data.source_file).lower().endswith(('.xlsx', '.xls')) else "发布报告数据"
        lines.append(f"\n**对比方式**: {self.curr_year}年调研数据 vs {self.prev_year}年{prev_label}")
        lines.append(f"\n**数据来源**:")
        lines.append(f"- {self.curr_year}年: 调研问卷 ({self.curr_summary['公司数']}家公司)")
        lines.append(f"- {self.prev_year}年: {prev_label} ({self.prev_data.source_file})")
        lines.append(f"\n生成时间: {datetime.datetime.now().strftime('%Y年%m月%d日')}")

        # 1. 招聘量分析
        lines.append(f"\n---\n## 1. 招聘量分析\n")
        lines.extend(self._section_volume_current())

        # 2. 渠道分布趋势
        lines.append(f"\n---\n## 2. 渠道分布趋势\n")
        lines.extend(self._section_channel())

        # 3. 招聘成本趋势
        lines.append(f"\n---\n## 3. 招聘成本趋势\n")
        lines.extend(self._section_cost())

        # 4. 成本结构趋势
        lines.append(f"\n---\n## 4. 成本结构趋势\n")
        lines.extend(self._section_cost_structure())

        # 5. 招聘周期趋势
        lines.append(f"\n---\n## 5. 招聘周期趋势\n")
        lines.extend(self._section_tth())

        # 6. 商业/研发细分趋势
        lines.append(f"\n---\n## 6. 细分职能趋势\n")
        lines.extend(self._section_detail())

        # 7. TA生产率趋势
        lines.append(f"\n---\n## 7. TA生产率趋势\n")
        lines.extend(self._section_productivity())

        # 8. TA人员配置分析
        lines.append(f"\n---\n## 8. TA人员配置分析\n")
        lines.extend(self._section_ta_config())

        if self.survey_sections.get('sheet4'):
            lines.append(f"\n---\n## 9. 2026新增HC预测与热点岗位前瞻（Sheet 4）\n")
            lines.append(self.survey_sections['sheet4'])

        # 9. 关键发现
        findings_no = 10 if self.survey_sections.get('sheet4') else 9
        lines.append(f"\n---\n## {findings_no}. 关键发现\n")
        lines.extend(self._section_findings())

        next_no = findings_no + 1
        if self.survey_sections.get('sheet3'):
            lines.append(f"\n---\n## {next_no}. TA招聘实践趋势分析（Sheet 3）\n")
            lines.append(self.survey_sections['sheet3'])
            next_no += 1

        if self.survey_sections.get('sheet5'):
            lines.append(f"\n---\n## {next_no}. 高管任期变化趋势分析（Sheet 5）\n")
            lines.append(self.survey_sections['sheet5'])
            next_no += 1

        if self.survey_trend_report:
            lines.append(f"\n---\n## {next_no}. TA招聘实践趋势分析（Sheet 3）\n")
            lines.append(self.survey_trend_report)
            next_no += 1

        lines.append(f"\n---\n## {next_no}. 数据提取审核\n")
        lines.extend(self._section_audit())

        return "\n".join(lines)

    def _section_volume_current(self):
        """当前年度招聘量分析，0值按未填写trim。"""
        lines = []
        overall = self.curr_df[self.curr_df['层级'] == '公司整体'].copy()
        hires = _positive_series(overall['招聘总量']) if '招聘总量' in overall else pd.Series(dtype=float)
        if len(hires) == 0:
            lines.append("*暂无有效招聘量样本。*")
            return lines

        lines.append("### 招聘量总体概览（0值已trim）\n")
        lines.append(f"- 有效公司样本: **{len(hires)}家**")
        lines.append(f"- 总招聘量: **{hires.sum():.0f}人**")
        lines.append(f"- 公司平均招聘量: **{hires.mean():.0f}人**")
        lines.append(f"- 公司招聘量P50: **{hires.median():.1f}人**")

        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        lines.append("\n### 各职能招聘量占比P50（基于公司内占比，0值已trim）\n")
        lines.append("| 职能 | P50 | 有效样本 |")
        lines.append("|------|------|------|")
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            ratios = []
            fd = func_df[func_df['职能'] == func]
            for _, row in fd.iterrows():
                total = _positive_series(overall[overall['公司'] == row['公司']]['招聘总量'])
                hire = pd.to_numeric(row.get('招聘总量'), errors='coerce')
                if len(total) > 0 and pd.notna(hire) and hire > 0:
                    ratios.append(hire / total.iloc[0])
            lines.append(f"| {func} | {_fmt_pct(np.median(ratios) if ratios else np.nan)} | {len(ratios)} |")

        if self.prev_data.func_volume_ratio_a or self.prev_data.func_volume_ratio_b:
            lines.append("\n### 不同规模公司各职能招聘量占比P50对比（A/B类）\n")
            lines.append("| 职能 | 2024 A类 | 2025 A类 | A类变化 | 2024 B类 | 2025 B类 | B类变化 |")
            lines.append("|------|------|------|------|------|------|------|")
            for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                prev_a = self.prev_data.func_volume_ratio_a.get(func, np.nan)
                curr_a = self._curr_func_volume_ratio(func, 'A')
                delta_a, _ = _delta_pct(curr_a, prev_a)
                prev_b = self.prev_data.func_volume_ratio_b.get(func, np.nan)
                curr_b = self._curr_func_volume_ratio(func, 'B')
                delta_b, _ = _delta_pct(curr_b, prev_b)
                lines.append(f"| {func} | {_fmt_pct(prev_a)} | {_fmt_pct(curr_a)} | {delta_a} | {_fmt_pct(prev_b)} | {_fmt_pct(curr_b)} | {delta_b} |")

        return lines

    def _section_tth(self):
        """招聘周期对比"""
        lines = []
        funcs = ['早期研发', '临床开发', '商业', '生产及供应链', '职能']
        if self.prev_data.func_tth:
            lines.append(f"### 各职能招聘周期 P50 对比（天）\n")
            lines.append(f"| 职能 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for func in funcs:
                prev = self.prev_data.func_tth.get(func, np.nan)
                curr = self._curr_tth(func)
                delta, arrow = _delta(curr, prev)
                lines.append(f"| {func} | {_fmt_num(prev)} | {_fmt_num(curr)} | {delta} | {arrow} |")
        else:
            lines.append(f"### {self.curr_year}年各职能招聘周期 P50（天）\n")
            lines.append("> 上年度最终口径表不含该维度，因此该表仅展示当年调研数据。\n")
            lines.append(f"| 职能 | {self.curr_year}(调研) |")
            lines.append(f"|------|------|")
            for func in funcs:
                curr = self._curr_tth(func)
                lines.append(f"| {func} | {_fmt_num(curr)} |")

        if self.prev_data.level_tth:
            lines.append(f"\n### 各职级招聘周期 P50 对比（天）\n")
            lines.append(f"| 职级 | {self.prev_year}(最终口径表) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for level in ['VP and Above', 'D-ED', 'M-AD', 'General']:
                prev = self.prev_data.level_tth.get(level, np.nan)
                curr = self._curr_level_tth(level)
                delta, arrow = _delta(curr, prev)
                lines.append(f"| {level} | {_fmt_num(prev)} | {_fmt_num(curr)} | {delta} | {arrow} |")

        if self.prev_data.func_tth_a or self.prev_data.func_tth_b:
            lines.append(f"\n### 不同规模公司各职能招聘周期P50对比（A/B类，天）\n")
            lines.append("| 职能 | 2024 A类 | 2025 A类 | A类变化 | 2024 B类 | 2025 B类 | B类变化 |")
            lines.append("|------|------|------|------|------|------|------|")
            for func in funcs:
                prev_a = self.prev_data.func_tth_a.get(func, np.nan)
                curr_a = self._curr_tth(func, 'A')
                delta_a, _ = _delta(curr_a, prev_a)
                prev_b = self.prev_data.func_tth_b.get(func, np.nan)
                curr_b = self._curr_tth(func, 'B')
                delta_b, _ = _delta(curr_b, prev_b)
                lines.append(f"| {func} | {_fmt_num(prev_a)} | {_fmt_num(curr_a)} | {delta_a} | {_fmt_num(prev_b)} | {_fmt_num(curr_b)} | {delta_b} |")

        return lines

    def _section_cost(self):
        """人均招聘成本对比"""
        lines = []
        funcs = ['早期研发', '临床开发', '商业', '生产及供应链', '职能']
        if self.prev_data.func_cost_per_hire:
            lines.append(f"### 各职能人均招聘成本 P50 对比（万元）\n")
            lines.append("> 口径：各职能外部渠道费用成本 / 各职能（猎头+内部推荐+RPO+主动投递）的招聘总数。\n")
            lines.append(f"| 职能 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for func in funcs:
                prev = self.prev_data.func_cost_per_hire.get(func, np.nan)
                curr = self._curr_cost(func)
                delta, arrow = _delta(curr, prev)
                lines.append(f"| {func} | {_fmt_num(prev, 2)} | {_fmt_num(curr, 2)} | {delta} | {arrow} |")
        elif getattr(self.prev_data, 'func_cost_ratio', None):
            lines.append(f"### 各职能招聘成本占比 P50 对比\n")
            lines.append(f"| 职能 | {self.prev_year}(最终报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for func in funcs:
                prev = self.prev_data.func_cost_ratio.get(func, np.nan)
                curr = self._curr_cost_ratio(func)
                delta, arrow = _delta_pct(curr, prev)
                lines.append(f"| {func} | {_fmt_pct(prev)} | {_fmt_pct(curr)} | {delta} | {arrow} |")

            lines.append(f"\n### {self.curr_year}年各职能人均招聘成本 P50（万元，0值已trim）\n")
            lines.append("> 2024最终口径表仅提供成本占比和渠道成本结构，未提供可直接对比的人均招聘成本；本表仅展示2025调研数据。\n")
            lines.append("> 口径：各职能外部渠道费用成本 / 各职能（猎头+内部推荐+RPO+主动投递）的招聘总数。\n")
            lines.append(f"| 职能 | {self.curr_year}(调研) | 有效样本 |")
            lines.append(f"|------|------|------|")
            for func in funcs:
                curr, n = self._curr_cost_with_n(func)
                lines.append(f"| {func} | {_fmt_num(curr, 2)} | {n} |")
        else:
            lines.append(f"### {self.curr_year}年各职能人均招聘成本 P50（万元）\n")
            lines.append("> 上年度最终口径表不含该维度，因此该表仅展示当年调研数据。\n")
            lines.append("> 口径：各职能外部渠道费用成本 / 各职能（猎头+内部推荐+RPO+主动投递）的招聘总数。\n")
            lines.append(f"| 职能 | {self.curr_year}(调研) |")
            lines.append(f"|------|------|")
            for func in funcs:
                curr = self._curr_cost(func)
                lines.append(f"| {func} | {_fmt_num(curr, 2)} |")

        return lines

    def _section_channel(self):
        """渠道分布对比"""
        lines = []
        lines.append(f"### 渠道分布 P50 对比\n")
        lines.append(
            "> 注：一级渠道是闭合构成，但下表的 P50 是分别对每个渠道的公司占比取中位数，"
            "三个 P50 可能来自不同公司，因此不要求相加为100%，也可能出现三个P50同向变化。"
            "如需判断结构是否整体迁移，请同时查看 P25/P75 或同公司样本。"
            "\n"
        )
        lines.append(f"| 渠道 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")

        # 当年渠道计算
        curr_ch = self._curr_channel_distribution()

        for ch_label, prev_key, curr_key in [
            ('HR直招', 'HR直招', ('整体', 'HR直招')),
            ('外部渠道', '外部渠道', ('整体', '外部渠道')),
            ('内部渠道', '内部渠道', ('整体', '内部渠道')),
        ]:
            prev = self.prev_data.channel_distribution.get(prev_key, np.nan)
            note = self.prev_data.value_notes.get(f"channel_distribution.{prev_key}") if hasattr(self.prev_data, 'value_notes') else None
            curr = curr_ch.get(curr_key, np.nan) if isinstance(curr_key, tuple) else np.nan
            delta, arrow = _delta_pct(curr, prev)
            lines.append(f"| {ch_label} | {_fmt_pct_with_note(prev, note)} | {_fmt_pct(curr)} | {delta} | {arrow} |")

        lines.append("\n### 外部渠道细分 P50 对比（占外部渠道）\n")
        lines.append(f"| 外部渠道 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for ch_label, prev_key, curr_key in [
            ('猎头', '猎头', ('整体', '猎头')),
            ('内推', '内推占外部', ('整体', '内推')),
            ('主动投递', '主动投递', ('整体', '主动投递')),
            ('校招', '校招', ('整体', '校招')),
            ('RPO', 'RPO', ('整体', 'RPO')),
        ]:
            prev = self.prev_data.channel_distribution.get(prev_key, np.nan)
            note = self.prev_data.value_notes.get(f"channel_distribution.{prev_key}") if hasattr(self.prev_data, 'value_notes') else None
            curr = curr_ch.get(curr_key, np.nan) if isinstance(curr_key, tuple) else np.nan
            delta, arrow = _delta_pct(curr, prev)
            lines.append(f"| {ch_label} | {_fmt_pct_with_note(prev, note)} | {_fmt_pct(curr)} | {delta} | {arrow} |")

        if self.prev_data.channel_distribution_a or self.prev_data.channel_distribution_b:
            lines.append("\n> 注：2024年最终口径表中“内部推荐”整体P50缺失时，使用平均值替代并在表格中标注；A/B类表保留其P50口径。\n")
            lines.append("### 不同规模公司一级渠道P50对比（A/B类）\n")
            lines.append("| 渠道 | 2024 A类 | 2025 A类 | A类变化 | 2024 B类 | 2025 B类 | B类变化 |")
            lines.append("|------|------|------|------|------|------|------|")
            for label, key, curr_key in [
                ('HR直招', 'HR直招', 'HR直招'),
                ('外部渠道', '外部渠道', '外部渠道'),
                ('内部渠道', '内部渠道', '内部渠道'),
            ]:
                prev_a = self.prev_data.channel_distribution_a.get(key, np.nan)
                curr_a = curr_ch.get(('A', curr_key), np.nan)
                delta_a, _ = _delta_pct(curr_a, prev_a)
                prev_b = self.prev_data.channel_distribution_b.get(key, np.nan)
                curr_b = curr_ch.get(('B', curr_key), np.nan)
                delta_b, _ = _delta_pct(curr_b, prev_b)
                lines.append(f"| {label} | {_fmt_pct(prev_a)} | {_fmt_pct(curr_a)} | {delta_a} | {_fmt_pct(prev_b)} | {_fmt_pct(curr_b)} | {delta_b} |")

            lines.append("\n### 不同规模公司外部渠道细分P50对比（A/B类，不含RPO拆分）\n")
            lines.append("| 外部渠道 | 2024 A类 | 2025 A类 | A类变化 | 2024 B类 | 2025 B类 | B类变化 |")
            lines.append("|------|------|------|------|------|------|------|")
            for label, key, curr_key in [
                ('猎头', '猎头', '猎头'),
                ('内推', '内推占外部', '内推'),
                ('主动投递', '主动投递', '主动投递'),
                ('校招', '校招', '校招'),
            ]:
                prev_a = self.prev_data.channel_distribution_a.get(key, np.nan)
                curr_a = curr_ch.get(('A', curr_key), np.nan)
                delta_a, _ = _delta_pct(curr_a, prev_a)
                prev_b = self.prev_data.channel_distribution_b.get(key, np.nan)
                curr_b = curr_ch.get(('B', curr_key), np.nan)
                delta_b, _ = _delta_pct(curr_b, prev_b)
                lines.append(f"| {label} | {_fmt_pct(prev_a)} | {_fmt_pct(curr_a)} | {delta_a} | {_fmt_pct(prev_b)} | {_fmt_pct(curr_b)} | {delta_b} |")

        return lines

    def _section_productivity(self):
        """TA生产率对比"""
        lines = []
        lines.append(f"### TA人均招聘量 P50 对比\n")
        lines.append(f"| 分组 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")

        curr_prod = self._curr_productivity()
        for group, label in [('整体', '整体'), ('A', 'A类'), ('B', 'B类')]:
            prev = self.prev_data.ta_productivity.get(group, np.nan)
            curr = curr_prod.get(group, np.nan)
            delta, arrow = _delta(curr, prev)
            lines.append(f"| {label} | {_fmt_num(prev)} | {_fmt_num(curr)} | {delta} | {arrow} |")

        return lines

    def _section_ta_config(self):
        """TA人员配置分析：TA FTE与第三方TA/RPO配置。"""
        lines = []
        summary = self._curr_ta_config_summary()
        if not summary:
            lines.append("*暂无有效TA人员配置样本。*")
            return lines

        lines.append("### 2025 TA配置P50（0值已trim）\n")
        lines.append("| 配置维度 | TA FTE P50 | TA FTE有效样本 | 第三方TA/RPO P50 | 第三方有效样本 |")
        lines.append("|------|------|------|------|------|")
        for label in ['公司整体', 'COE function', 'TA BP', '早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            data = summary.get(label, {})
            fte = data.get('TA_FTE_P50', np.nan)
            third = data.get('TA_第三方_P50', np.nan)
            fte_n = data.get('TA_FTE_n', 0)
            third_n = data.get('TA_第三方_n', 0)
            lines.append(
                f"| {label} | {_fmt_num(fte, 2)} | {fte_n} | {_fmt_num(third, 2)} | {third_n} |"
            )

        lines.append("\n> TA生产率需结合TA FTE、第三方TA/RPO、COE function和TA BP配置共同解读，不宜只看人均招聘量。")

        lines.append("\n### 2025 不同规模公司TA配置P50（A/B类，0值已trim）\n")
        lines.append("| 配置维度 | A类TA FTE P50 | A类样本 | B类TA FTE P50 | B类样本 | A类第三方/RPO P50 | B类第三方/RPO P50 |")
        lines.append("|------|------|------|------|------|------|------|")
        summary_a = self._curr_ta_config_summary(scale='A')
        summary_b = self._curr_ta_config_summary(scale='B')
        for label in ['公司整体', 'COE function', 'TA BP', '早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            a = summary_a.get(label, {})
            b = summary_b.get(label, {})
            lines.append(
                f"| {label} | {_fmt_num(a.get('TA_FTE_P50', np.nan), 2)} | {a.get('TA_FTE_n', 0)} | "
                f"{_fmt_num(b.get('TA_FTE_P50', np.nan), 2)} | {b.get('TA_FTE_n', 0)} | "
                f"{_fmt_num(a.get('TA_第三方_P50', np.nan), 2)} | {_fmt_num(b.get('TA_第三方_P50', np.nan), 2)} |"
            )

        coe = summary.get('COE function', {})
        ta_bp = summary.get('TA BP', {})
        lines.append(
            f"\n> COE function有效样本{coe.get('TA_FTE_n', 0)}家，TA BP有效样本{ta_bp.get('TA_FTE_n', 0)}家；"
            "该表用于补充解释TA生产率差异，体现不同规模公司在TA配置、COE覆盖和第三方/RPO支持上的差异。"
        )
        lines.append("\n> 2024 TA配置数据仅覆盖COE/BP图片口径，不完整，因此不做年度对比。")
        return lines

    def _section_detail(self):
        """细分职能对比"""
        lines = []

        # 商业细分
        if self.prev_data.commercial_detail:
            lines.append(f"### 商业二级职能招聘周期 P50 对比（天）\n")
            lines.append(f"| 二级职能 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|----------|------|------|------|------|")

            curr_comm = self.curr_df[(self.curr_df['层级'] == '商业细分') & (self.curr_df['职级'] == '整体')]
            all_funcs = sorted(
                set(self.prev_data.commercial_detail.keys()) |
                set(curr_comm['职能'].unique() if not curr_comm.empty else [])
            )

            for func in all_funcs:
                prev = self.prev_data.commercial_detail.get(func, {}).get('招聘周期', np.nan)
                curr_tth = _positive_series(curr_comm[curr_comm['职能'] == func]['招聘周期_天'])
                curr = curr_tth.median() if len(curr_tth) > 0 else np.nan
                delta, arrow = _delta(curr, prev)
                lines.append(f"| {func} | {_fmt_num(prev)} | {_fmt_num(curr)} | {delta} | {arrow} |")

        return lines

    def _section_cost_structure(self):
        """成本结构对比"""
        lines = []
        if self.prev_data.cost_structure:
            lines.append(f"### 渠道成本结构 P50 对比\n")
            lines.append(f"| 指标 | {self.prev_year}(报告) | {self.curr_year}(调研) | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")

            curr_struct = self._curr_cost_structure()
            for key in ['猎头费占比', '内推费占比', 'RPO费占比']:
                prev = self.prev_data.cost_structure.get(key, np.nan)
                curr = curr_struct.get(key, np.nan)
                delta, arrow = _delta_pct(curr, prev)
                note = self.prev_data.value_notes.get(f"cost_structure.{key}", "") if hasattr(self.prev_data, "value_notes") else ""
                lines.append(f"| {key} | {_fmt_pct_with_note(prev, note)} | {_fmt_pct(curr)} | {delta} | {arrow} |")

            if self.prev_data.cost_structure_a or self.prev_data.cost_structure_b:
                lines.append("\n### 不同规模公司渠道成本结构P50对比（A/B类）\n")
                lines.append("| 指标 | 2024 A类 | 2025 A类 | A类变化 | 2024 B类 | 2025 B类 | B类变化 |")
                lines.append("|------|------|------|------|------|------|------|")
                for key in ['猎头费占比', '内推费占比', 'RPO费占比']:
                    prev_a = self.prev_data.cost_structure_a.get(key, np.nan)
                    curr_a = curr_struct.get(('A', key), np.nan)
                    delta_a, _ = _delta_pct(curr_a, prev_a)
                    prev_b = self.prev_data.cost_structure_b.get(key, np.nan)
                    curr_b = curr_struct.get(('B', key), np.nan)
                    delta_b, _ = _delta_pct(curr_b, prev_b)
                    note_a = self.prev_data.value_notes.get(f"cost_structure_A类.{key}", "") if hasattr(self.prev_data, "value_notes") else ""
                    note_b = self.prev_data.value_notes.get(f"cost_structure_B类.{key}", "") if hasattr(self.prev_data, "value_notes") else ""
                    lines.append(f"| {key} | {_fmt_pct_with_note(prev_a, note_a)} | {_fmt_pct(curr_a)} | {delta_a} | {_fmt_pct_with_note(prev_b, note_b)} | {_fmt_pct(curr_b)} | {delta_b} |")

        return lines

    def _section_findings(self):
        """关键发现"""
        lines = []
        findings = []

        # 招聘周期变化
        for func in ['商业', '临床开发', '早期研发', '生产及供应链', '职能']:
            prev = self.prev_data.func_tth.get(func, np.nan)
            curr = self._curr_tth(func)
            if pd.notna(prev) and pd.notna(curr) and prev > 0:
                change = (curr - prev) / prev
                if abs(change) > 0.1:
                    direction = "延长" if change > 0 else "缩短"
                    findings.append(
                        f"**{func}招聘周期{direction}**: 从{prev:.0f}天{direction}至{curr:.0f}天 ({change:+.1%})"
                    )

        # 成本变化
        for func in ['商业', '临床开发', '早期研发']:
            prev = self.prev_data.func_cost_per_hire.get(func, np.nan)
            curr = self._curr_cost(func)
            if pd.notna(prev) and pd.notna(curr) and prev > 0:
                change = (curr - prev) / prev
                if abs(change) > 0.15:
                    direction = "上升" if change > 0 else "下降"
                    findings.append(
                        f"**{func}人均成本{direction}**: 从{prev:.2f}万{direction}至{curr:.2f}万 ({change:+.1%})"
                    )

        # TA生产率
        curr_prod = self._curr_productivity()
        for group, label in [('整体', '整体')]:
            prev = self.prev_data.ta_productivity.get(group, np.nan)
            curr = curr_prod.get(group, np.nan)
            if pd.notna(prev) and pd.notna(curr) and prev > 0:
                change = (curr - prev) / prev
                if abs(change) > 0.1:
                    direction = "提升" if change > 0 else "下降"
                    findings.append(
                        f"**TA生产率{direction}**: 从{prev:.1f}{direction}至{curr:.1f} ({change:+.1%})"
                    )

        curr_ch = self._curr_channel_distribution()
        prev_ref = self.prev_data.channel_distribution.get('内推占外部', np.nan)
        curr_ref = curr_ch.get(('整体', '内推'), np.nan)
        if pd.notna(prev_ref) and pd.notna(curr_ref):
            note = self.prev_data.value_notes.get('channel_distribution.内推占外部', '') if hasattr(self.prev_data, 'value_notes') else ''
            findings.append(
                f"**内推渠道占比变化需按口径解读**: 2024为{_fmt_pct(prev_ref)}"
                f"{'（' + note + '）' if note else ''}，2025为{_fmt_pct(curr_ref)}。"
            )

        curr_prod = self._curr_productivity()
        if pd.notna(curr_prod.get('A', np.nan)) and pd.notna(curr_prod.get('B', np.nan)):
            findings.append(
                f"**A/B类比较已补充**: 2025年A类TA生产率P50为{curr_prod.get('A'):.1f}，"
                f"B类为{curr_prod.get('B'):.1f}；差异来自问卷中的公司规模字段，不是2025缺少A/B数据。"
            )

        ta_summary = self._curr_ta_config_summary()
        coe = ta_summary.get('COE function', {})
        bp = ta_summary.get('TA BP', {})
        if coe or bp:
            findings.append(
                f"**TA配置需要与生产率联动解读**: COE function有效样本{coe.get('TA_FTE_n', 0)}家，"
                f"TA BP有效样本{bp.get('TA_FTE_n', 0)}家，配置差异会影响不同规模公司的招聘承载能力。"
            )

        if findings:
            for i, f in enumerate(findings, 1):
                lines.append(f"{i}. {f}")
        else:
            lines.append("*两年数据对比未发现显著变化趋势*")

        return lines

    def _section_audit(self):
        """数据提取审核"""
        lines = []
        lines.append(f"以下是从上年度最终口径表中提取的原始数据点，请核实准确性：\n")
        lines.append(f"| # | 提取内容 |")
        lines.append(f"|---|---------|")
        for i, ext in enumerate(self.prev_data.raw_extractions, 1):
            lines.append(f"| {i} | {ext} |")
        return lines

    # ==================== 当年数据计算辅助 ====================

    def _curr_tth(self, func, scale=None):
        """当年某职能招聘周期P50"""
        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        if scale:
            func_df = func_df[func_df['规模'] == scale]
        fd = func_df[func_df['职能'] == func]
        tth = _positive_series(fd['招聘周期_天'])
        return tth.median() if len(tth) > 0 else np.nan

    def _curr_func_volume_ratio(self, func, scale=None):
        """当年某职能招聘量占比P50。"""
        overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        if scale:
            overall = overall[overall['规模'] == scale]
            func_df = func_df[func_df['规模'] == scale]
        fd = func_df[func_df['职能'] == func]
        ratios = []
        for _, row in fd.iterrows():
            total = _positive_series(overall[overall['公司'] == row['公司']]['招聘总量'])
            hire = pd.to_numeric(row.get('招聘总量'), errors='coerce')
            if len(total) > 0 and pd.notna(hire) and hire > 0:
                ratios.append(hire / total.iloc[0])
        return np.median(ratios) if ratios else np.nan

    def _curr_level_tth(self, level):
        """当年某职级招聘周期P50"""
        level_df = self.curr_df[self.curr_df['层级'] == '职级']
        ld = level_df[level_df['职级'] == level]
        tth = _positive_series(ld['招聘周期_天'])
        return tth.median() if len(tth) > 0 else np.nan

    def _curr_cost(self, func):
        """当年某职能人均招聘成本P50"""
        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        fd = func_df[func_df['职能'] == func]
        costs = []
        for _, row in fd.iterrows():
            cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            external_hires = _external_cost_hire_count(row)
            if pd.notna(cost) and cost > 0 and external_hires > 0:
                costs.append(cost / external_hires)
        return np.median(costs) if costs else np.nan

    def _curr_cost_with_n(self, func):
        """当年某职能人均招聘成本P50及有效样本。"""
        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        fd = func_df[func_df['职能'] == func]
        costs = []
        for _, row in fd.iterrows():
            cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            external_hires = _external_cost_hire_count(row)
            if pd.notna(cost) and cost > 0 and external_hires > 0:
                costs.append(cost / external_hires)
        return (np.median(costs), len(costs)) if costs else (np.nan, 0)

    def _curr_cost_ratio(self, func):
        """当年某职能招聘成本占比P50，口径对齐最终报告表。"""
        overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        func_df = self.curr_df[(self.curr_df['层级'] == '一级职能') & (self.curr_df['职级'] == '整体')]
        fd = func_df[func_df['职能'] == func]
        ratios = []
        for _, row in fd.iterrows():
            company = row.get('公司')
            total_costs = overall[overall['公司'] == company]['外部渠道成本_万'] if '外部渠道成本_万' in overall else pd.Series(dtype=float)
            total_costs = _positive_series(total_costs)
            cost = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
            if len(total_costs) and pd.notna(cost) and cost > 0:
                ratios.append(cost / total_costs.iloc[0])
        return np.median(ratios) if ratios else np.nan

    def _curr_channel_distribution(self):
        """当年渠道分布"""
        overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        result = {}
        channel_data = []
        for _, row in overall.iterrows():
            hr_n = _safe(row.get('HR直招', 0))
            hh_n = _safe(row.get('猎头_人', 0))
            ref_n = _safe(row.get('内推_人', 0))
            transfer_n = _safe(row.get('内部转岗', 0))
            rpo_n = _safe(row.get('RPO_人', 0))
            apply_n = _safe(row.get('主动投递', 0))
            campus_n = _safe(row.get('校招', 0))
            ext_n = hh_n + rpo_n + ref_n + apply_n + campus_n
            total = hr_n + ext_n + transfer_n
            if total <= 0:
                continue
            hr = hr_n / total
            transfer = transfer_n / total
            ext = ext_n / total
            hh = hh_n / ext_n if ext_n > 0 else np.nan
            ref = ref_n / ext_n if ext_n > 0 else np.nan
            rpo = rpo_n / ext_n if ext_n > 0 else np.nan
            apply_d = apply_n / ext_n if ext_n > 0 else np.nan
            campus = campus_n / ext_n if ext_n > 0 else np.nan
            channel_data.append({
                '规模': row['规模'],
                'HR直招': hr, '外部渠道': ext, '内部渠道': transfer,
                '猎头': hh, '内推': ref, '主动投递': apply_d, '校招': campus, 'RPO': rpo,
            })
        if channel_data:
            chdf = pd.DataFrame(channel_data)
            for ch in ['HR直招', '外部渠道', '内部渠道', '猎头', '内推', '主动投递', '校招', 'RPO']:
                s = _positive_series(chdf[ch])
                result[('整体', ch)] = s.median() if len(s) else np.nan
                if ch != 'RPO':
                    for scale in ['A', 'B']:
                        sub = chdf[chdf['规模'] == scale]
                        ss = _positive_series(sub[ch])
                        result[(scale, ch)] = ss.median() if len(ss) > 0 else np.nan
        return result

    def _curr_productivity(self):
        """当年TA生产率"""
        ta_df = self.curr_df[self.curr_df['层级'] == 'TA配置']
        overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        result = {}
        prod_data = []
        for co in self.curr_agg.companies:
            co_ta = ta_df[(ta_df['公司'] == co) & (ta_df['职能'].str.contains('公司整体', na=False))]
            co_hire = overall[overall['公司'] == co]
            if not co_ta.empty and not co_hire.empty:
                ta_fte = pd.to_numeric(co_ta.iloc[0].get('TA_FTE'), errors='coerce')
                hire = co_hire.iloc[0].get('招聘总量', 0)
                if pd.notna(ta_fte) and ta_fte > 0 and hire > 0:
                    prod_data.append({'规模': co_hire.iloc[0]['规模'], '人均招聘量': hire / ta_fte})
        if prod_data:
            pdf = pd.DataFrame(prod_data)
            result['整体'] = _positive_series(pdf['人均招聘量']).median()
            for scale in ['A', 'B']:
                sub = pdf[pdf['规模'] == scale]
                ss = _positive_series(sub['人均招聘量'])
                result[scale] = ss.median() if len(ss) > 0 else np.nan
        return result

    def _calc_channel_closure(self, df) -> Dict[str, float]:
        """Check whether HR direct + external + internal channels close to total hires."""
        overall = df[df['层级'] == '公司整体']
        ratios = []
        total_hires = 0.0
        total_channels = 0.0
        for _, row in overall.iterrows():
            total = _safe(row.get('招聘总量', 0))
            if total <= 0:
                continue
            hr = _safe(row.get('HR直招', 0))
            hh = _safe(row.get('猎头_人', 0))
            ref = _safe(row.get('内推_人', 0))
            transfer = _safe(row.get('内部转岗', 0))
            rpo = _safe(row.get('RPO_人', 0))
            apply_d = _safe(row.get('主动投递', 0))
            campus = _safe(row.get('校招', 0))
            channel_sum = hr + hh + ref + transfer + rpo + apply_d + campus
            ratios.append(channel_sum / total)
            total_hires += total
            total_channels += channel_sum
        return {
            'p50': np.median(ratios) if ratios else np.nan,
            'weighted': total_channels / total_hires if total_hires > 0 else np.nan,
        }

    def _ta_config_label(self, func_name: str) -> Optional[str]:
        name = str(func_name or '')
        if '公司整体' in name:
            return '公司整体'
        if 'COE' in name:
            return 'COE function'
        if 'TA BP' in name or 'TABP' in name:
            return 'TA BP'
        if '早期研发' in name or 'Discovery' in name:
            return '早期研发'
        if '临床开发' in name or 'Clinical' in name:
            return '临床开发'
        if '商业' in name or 'Commercial' in name:
            return '商业'
        if '生产' in name or '供应链' in name or 'Manufacturing' in name or 'Supply' in name:
            return '生产及供应链'
        if '职能' in name or 'Enabling' in name:
            return '职能'
        return None

    def _curr_ta_config_summary(self, scale=None):
        """当前年度TA人员配置P50，0值trim。"""
        ta_df = self.curr_df[self.curr_df['层级'] == 'TA配置']
        if scale:
            ta_df = ta_df[ta_df['规模'] == scale]
        if ta_df.empty:
            return {}
        records = []
        for _, row in ta_df.iterrows():
            label = self._ta_config_label(row.get('职能'))
            if not label:
                continue
            records.append({
                '配置维度': label,
                'TA_FTE': pd.to_numeric(row.get('TA_FTE'), errors='coerce'),
                'TA_第三方': pd.to_numeric(row.get('TA_第三方'), errors='coerce'),
            })
        if not records:
            return {}
        df = pd.DataFrame(records)
        result = {}
        for label, sub in df.groupby('配置维度'):
            fte = _positive_series(sub['TA_FTE'])
            third = _positive_series(sub['TA_第三方'])
            result[label] = {
                'TA_FTE_P50': fte.median() if len(fte) else np.nan,
                'TA_FTE_n': len(fte),
                'TA_第三方_P50': third.median() if len(third) else np.nan,
                'TA_第三方_n': len(third),
            }
        return result

    def _curr_cost_structure(self):
        """当年渠道成本结构"""
        overall = self.curr_df[self.curr_df['层级'] == '公司整体']
        result = {}
        metric_cols = {
            '猎头费占比': '猎头费_万',
            '内推费占比': '内推费_万',
            'RPO费占比': 'RPO费_万',
        }
        for key, col in metric_cols.items():
            ratios = []
            scale_ratios = {'A': [], 'B': []}
            for _, row in overall.iterrows():
                tc = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
                cost = pd.to_numeric(row.get(col), errors='coerce')
                if pd.notna(tc) and tc > 0 and pd.notna(cost) and cost > 0:
                    ratio = cost / tc
                    ratios.append(ratio)
                    scale = str(row.get('规模', '')).strip().upper()
                    if scale in scale_ratios:
                        scale_ratios[scale].append(ratio)
            result[key] = np.median(ratios) if ratios else np.nan
            for scale, values in scale_ratios.items():
                result[(scale, key)] = np.median(values) if values else np.nan
        return result

    def export_comparison_table(self) -> pd.DataFrame:
        """导出年度对比表"""
        rows = []

        # 招聘周期
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev = self.prev_data.func_tth.get(func, np.nan)
            curr = self._curr_tth(func)
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': '招聘周期', '维度': func,
                f'{self.prev_year}(报告)': _fmt_num(prev),
                f'{self.curr_year}(调研)': _fmt_num(curr),
                '变化': delta, '趋势': arrow,
            })

        # 成本
        for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
            prev = self.prev_data.func_cost_per_hire.get(func, np.nan)
            curr = self._curr_cost(func)
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': '人均成本', '维度': func,
                f'{self.prev_year}(报告)': _fmt_num(prev, 2),
                f'{self.curr_year}(调研)': _fmt_num(curr, 2),
                '变化': delta, '趋势': arrow,
            })

        # 渠道分布
        curr_ch = self._curr_channel_distribution()
        for ch_label, prev_key, curr_key in [
            ('HR直招', 'HR直招', ('整体', 'HR直招')),
            ('外部渠道', '外部渠道', ('整体', '外部渠道')),
            ('内部渠道', '内部渠道', ('整体', '内部渠道')),
            ('猎头(占外部)', '猎头', ('整体', '猎头')),
            ('内推(占外部)', '内推占外部', ('整体', '内推')),
        ]:
            prev = self.prev_data.channel_distribution.get(prev_key, np.nan)
            curr = curr_ch.get(curr_key, np.nan) if isinstance(curr_key, tuple) else np.nan
            delta, arrow = _delta_pct(curr, prev)
            rows.append({
                '模块': '渠道分布', '维度': ch_label,
                f'{self.prev_year}(报告)': _fmt_pct(prev),
                f'{self.curr_year}(调研)': _fmt_pct(curr),
                '变化': delta, '趋势': arrow,
            })

        # TA生产率
        curr_prod = self._curr_productivity()
        for group, label in [('整体', '整体'), ('A', 'A类'), ('B', 'B类')]:
            prev = self.prev_data.ta_productivity.get(group, np.nan)
            curr = curr_prod.get(group, np.nan)
            delta, arrow = _delta(curr, prev)
            rows.append({
                '模块': 'TA生产率', '维度': label,
                f'{self.prev_year}(报告)': _fmt_num(prev),
                f'{self.curr_year}(调研)': _fmt_num(curr),
                '变化': delta, '趋势': arrow,
            })

        for level in ['VP and Above', 'D-ED', 'M-AD', 'General']:
            prev = self.prev_data.level_tth.get(level, np.nan)
            curr = self._curr_level_tth(level)
            if pd.notna(prev) or pd.notna(curr):
                delta, arrow = _delta(curr, prev)
                rows.append({
                    '模块': '招聘周期-职级', '维度': level,
                    f'{self.prev_year}(报告)': _fmt_num(prev),
                    f'{self.curr_year}(调研)': _fmt_num(curr),
                    '变化': delta, '趋势': arrow,
                })

        # 成本结构
        curr_struct = self._curr_cost_structure()
        for key in ['猎头费占比', '内推费占比', 'RPO费占比']:
            prev = self.prev_data.cost_structure.get(key, np.nan)
            curr = curr_struct.get(key, np.nan)
            delta, arrow = _delta_pct(curr, prev)
            note = self.prev_data.value_notes.get(f"cost_structure.{key}", "") if hasattr(self.prev_data, "value_notes") else ""
            rows.append({
                '模块': '渠道成本结构', '维度': key,
                f'{self.prev_year}(报告)': _fmt_pct_with_note(prev, note),
                f'{self.curr_year}(调研)': _fmt_pct(curr),
                '变化': delta, '趋势': arrow,
            })

        return pd.DataFrame(rows)
