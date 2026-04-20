"""
Indicator Pivot - 指标透视与交叉分析
从Wiki知识库中提取数据，按分析维度进行透视、对比、排序
生成可直接用于报告的结构化分析结果
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
from scheme.analysis_dimensions import DIMENSION_REGISTRY, DimensionConfig


class PivotResult:
    """透视分析结果"""

    def __init__(self, dimension: str, entry_id: str):
        self.dimension = dimension
        self.entry_id = entry_id
        self.data: Optional[pd.DataFrame] = None
        self.pivot_table: Optional[pd.DataFrame] = None
        self.rankings: Dict[str, List] = {}
        self.comparisons: Dict[str, Any] = {}
        self.highlights: List[str] = []

    def to_dict(self) -> Dict:
        return {
            'dimension': self.dimension,
            'entry_id': self.entry_id,
            'data_shape': self.data.shape if self.data is not None else None,
            'rankings': self.rankings,
            'comparisons': self.comparisons,
            'highlights': self.highlights,
        }


class IndicatorPivot:
    """指标透视引擎"""

    def __init__(self, kb: KnowledgeBase):
        self.kb = kb
        self.results: Dict[str, PivotResult] = {}

    def pivot_all(self) -> Dict[str, PivotResult]:
        """对所有可用维度执行透视分析"""
        for dim_config in DIMENSION_REGISTRY.get_available():
            entry = self.kb.get_entry(dim_config.entry_id)
            if entry is None:
                continue

            result = self._pivot_entry(entry, dim_config)
            if result:
                self.results[dim_config.entry_id] = result

        return self.results

    def pivot_by_module(self, module: str) -> Dict[str, PivotResult]:
        """按模块执行透视分析"""
        results = {}
        for dim_config in DIMENSION_REGISTRY.get_by_module(module):
            if not dim_config.is_available:
                continue
            entry = self.kb.get_entry(dim_config.entry_id)
            if entry is None:
                continue
            result = self._pivot_entry(entry, dim_config)
            if result:
                results[dim_config.entry_id] = result
        return results

    def _pivot_entry(self, entry: KnowledgeEntry, dim_config: DimensionConfig) -> Optional[PivotResult]:
        """对单个知识条目执行透视分析"""
        result = PivotResult(dim_config.dimension, dim_config.entry_id)

        # 获取数据
        data = entry.metric_value
        if isinstance(data, pd.DataFrame):
            result.data = data
        elif isinstance(data, dict):
            result.data = pd.DataFrame([data])
        else:
            return None

        if result.data.empty:
            return None

        # 执行透视
        self._compute_rankings(result, dim_config)
        self._compute_comparisons(result, dim_config)
        self._generate_highlights(result, dim_config)

        return result

    def _compute_rankings(self, result: PivotResult, dim_config: DimensionConfig):
        """计算排名"""
        df = result.data
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in numeric_cols:
            valid = df[df[col].notna()].copy()
            if valid.empty:
                continue

            # 找到主分组列（非数值列）
            group_cols = [c for c in df.columns if c not in numeric_cols]
            if not group_cols:
                continue

            primary_group = group_cols[0]
            sorted_df = valid.sort_values(col, ascending=False)

            result.rankings[col] = {
                'top': sorted_df.head(3)[[primary_group, col]].to_dict('records'),
                'bottom': sorted_df.tail(3)[[primary_group, col]].to_dict('records'),
                'max': {
                    'value': float(sorted_df[col].iloc[0]),
                    'label': str(sorted_df[primary_group].iloc[0]),
                },
                'min': {
                    'value': float(sorted_df[col].iloc[-1]),
                    'label': str(sorted_df[primary_group].iloc[-1]),
                },
                'median': float(valid[col].median()),
                'mean': float(valid[col].mean()),
            }

    def _compute_comparisons(self, result: PivotResult, dim_config: DimensionConfig):
        """计算对比分析（A类 vs B类公司）"""
        df = result.data
        if '公司规模' not in df.columns:
            return

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        group_cols = [c for c in df.columns if c not in numeric_cols and c != '公司规模']

        for col in numeric_cols:
            a_data = df[df['公司规模'] == 'A']
            b_data = df[df['公司规模'] == 'B']

            if a_data.empty or b_data.empty:
                continue

            comparison = {
                'A_mean': float(a_data[col].mean()),
                'B_mean': float(b_data[col].mean()),
                'diff': float(a_data[col].mean() - b_data[col].mean()),
            }

            # 如果有分组维度，计算每个分组的A/B差异
            if group_cols:
                primary = group_cols[0]
                pivot = df.pivot_table(
                    index=primary, columns='公司规模',
                    values=col, aggfunc='first'
                )
                if 'A' in pivot.columns and 'B' in pivot.columns:
                    pivot['差异'] = pivot['A'] - pivot['B']
                    pivot['差异_abs'] = pivot['差异'].abs()
                    max_diff_idx = pivot['差异_abs'].idxmax()
                    comparison['max_diff_group'] = str(max_diff_idx)
                    comparison['max_diff_value'] = float(pivot.loc[max_diff_idx, '差异'])
                    result.pivot_table = pivot

            result.comparisons[col] = comparison

    def _generate_highlights(self, result: PivotResult, dim_config: DimensionConfig):
        """生成关键发现"""
        highlights = []

        # 基于排名的发现
        for metric, ranking in result.rankings.items():
            max_info = ranking.get('max', {})
            min_info = ranking.get('min', {})

            if max_info and min_info:
                max_val = max_info['value']
                min_val = min_info['value']

                # 判断是百分比还是绝对值
                if 0 < max_val <= 1:
                    highlights.append(
                        f"【{max_info['label']}】{metric}最高 ({max_val:.1%})，"
                        f"【{min_info['label']}】最低 ({min_val:.1%})"
                    )
                else:
                    highlights.append(
                        f"【{max_info['label']}】{metric}最高 ({max_val:.1f})，"
                        f"【{min_info['label']}】最低 ({min_val:.1f})"
                    )

        # 基于对比的发现
        for metric, comp in result.comparisons.items():
            if 'max_diff_group' in comp:
                diff = comp['max_diff_value']
                if abs(diff) > 0:
                    direction = "高于" if diff > 0 else "低于"
                    if 0 < abs(comp['A_mean']) <= 1:
                        highlights.append(
                            f"A/B类公司在【{comp['max_diff_group']}】差异最大，"
                            f"A类{direction}B类 {abs(diff):.1%}"
                        )
                    else:
                        highlights.append(
                            f"A/B类公司在【{comp['max_diff_group']}】差异最大，"
                            f"A类{direction}B类 {abs(diff):.1f}"
                        )

        result.highlights = highlights

    def get_all_highlights(self) -> Dict[str, List[str]]:
        """获取所有维度的关键发现"""
        return {
            entry_id: result.highlights
            for entry_id, result in self.results.items()
            if result.highlights
        }

    def export_pivot_summary(self) -> str:
        """导出透视分析摘要"""
        lines = ["# 指标透视分析摘要", ""]

        for entry_id, result in self.results.items():
            dim_config = DIMENSION_REGISTRY.get_by_entry_id(entry_id)
            module = dim_config.module if dim_config else "未知"
            lines.append(f"\n## {module} - {result.dimension}")
            lines.append("")

            if result.highlights:
                for h in result.highlights:
                    lines.append(f"• {h}")
            else:
                lines.append("• （无显著发现）")

        return "\n".join(lines)
