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
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from multi_company import IndustryAggregator, IndustryReportGenerator


def _safe(val):
    """NaN-safe value extraction"""
    try:
        v = float(val)
        return v if pd.notna(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _fmt_pct(v):
    """Format as percentage"""
    return f"{v:.2%}" if pd.notna(v) and v != 0 else "N/A"


def _fmt_num(v, decimals=1):
    """Format number"""
    return f"{v:.{decimals}f}" if pd.notna(v) else "N/A"


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
        prev_total = prev_overall['招聘总量'].sum() if not prev_overall.empty else 0
        curr_total = curr_overall['招聘总量'].sum() if not curr_overall.empty else 0
        delta, arrow = _delta(curr_total, prev_total)
        lines.append(f"| 总招聘量 | {prev_total:.0f}人 | {curr_total:.0f}人 | {arrow}{delta} |")

        prev_avg = prev_overall['招聘总量'].mean() if not prev_overall.empty else 0
        curr_avg = curr_overall['招聘总量'].mean() if not curr_overall.empty else 0
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
        lines.append(f"| 渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
        lines.append(f"|------|------|------|------|------|")
        for ch in ['HR直招', '外部渠道', '内部渠道']:
            prev_v = prev_ch.get(('整体', ch), np.nan)
            curr_v = curr_ch.get(('整体', ch), np.nan)
            delta, arrow = _delta_pct(curr_v, prev_v)
            lines.append(f"| {ch} | {_fmt_pct(prev_v)} | {_fmt_pct(curr_v)} | {delta} | {arrow} |")

        # A类 vs B类 渠道变化
        for scale_label, scale_code in [('A类公司', 'A'), ('B类公司', 'B')]:
            lines.append(f"\n### {scale_label}渠道分布 P50 对比\n")
            lines.append(f"| 渠道 | {self.prev_year} | {self.curr_year} | 变化 | 趋势 |")
            lines.append(f"|------|------|------|------|------|")
            for ch in ['HR直招', '外部渠道', '内部渠道', '猎头', '内推']:
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
            total = _safe(row.get('招聘总量', 0))
            if total <= 0:
                continue
            hr = _safe(row.get('HR直招', 0)) / total
            hh = _safe(row.get('猎头_人', 0)) / total
            ref = _safe(row.get('内推_人', 0)) / total
            transfer = _safe(row.get('内部转岗', 0)) / total
            rpo = _safe(row.get('RPO_人', 0)) / total
            apply_d = _safe(row.get('主动投递', 0)) / total
            campus = _safe(row.get('校招', 0)) / total
            ext = hh + rpo + ref + apply_d + campus

            channel_data.append({
                '规模': row['规模'],
                'HR直招': hr, '外部渠道': ext, '内部渠道': transfer,
                '猎头': hh, '内推': ref,
            })

        if not channel_data:
            return result

        chdf = pd.DataFrame(channel_data)
        for ch in ['HR直招', '外部渠道', '内部渠道', '猎头', '内推']:
            result[('整体', ch)] = chdf[ch].median()
            for scale in ['A', 'B']:
                sub = chdf[chdf['规模'] == scale]
                result[(scale, ch)] = sub[ch].median() if len(sub) > 0 else np.nan

        return result

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
            total = pd.to_numeric(row.get('招聘总量'), errors='coerce')
            if pd.notna(cost) and pd.notna(total) and total > 0:
                costs.append(cost / total)
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
