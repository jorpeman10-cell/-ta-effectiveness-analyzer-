"""
Report Writer - 报告撰写引擎
基于Wiki知识库和透视分析结果，自动生成结构化的分析报告
支持Markdown和结构化JSON两种输出格式
"""
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
from scheme.analysis_dimensions import DIMENSION_REGISTRY
from scheme.indicator_pivot import IndicatorPivot, PivotResult


def _fmt_pct(v):
    """格式化百分比"""
    if pd.isna(v) or v is None:
        return 'N/A'
    return f'{v:.1%}'


def _fmt_num(v, unit=''):
    """格式化数值"""
    if pd.isna(v) or v is None:
        return 'N/A'
    return f'{v:.1f}{unit}'


class ReportSection:
    """报告章节"""

    def __init__(self, title: str, level: int = 2):
        self.title = title
        self.level = level
        self.paragraphs: List[str] = []
        self.tables: List[Dict] = []
        self.bullet_points: List[str] = []
        self.sub_sections: List['ReportSection'] = []

    def add_paragraph(self, text: str):
        self.paragraphs.append(text)

    def add_bullet(self, text: str):
        self.bullet_points.append(text)

    def add_table(self, df: pd.DataFrame, caption: str = ''):
        self.tables.append({'data': df, 'caption': caption})

    def add_sub_section(self, section: 'ReportSection'):
        self.sub_sections.append(section)

    def to_markdown(self) -> str:
        lines = [f"{'#' * self.level} {self.title}", ""]

        for p in self.paragraphs:
            lines.append(p)
            lines.append("")

        if self.bullet_points:
            for bp in self.bullet_points:
                lines.append(f"• {bp}")
            lines.append("")

        for tbl in self.tables:
            if tbl['caption']:
                lines.append(f"**{tbl['caption']}**")
                lines.append("")
            df = tbl['data']
            if isinstance(df, pd.DataFrame) and not df.empty:
                lines.append(df.to_markdown(index=False))
                lines.append("")

        for sub in self.sub_sections:
            lines.append(sub.to_markdown())

        return "\n".join(lines)


class ReportWriter:
    """报告撰写引擎"""

    def __init__(self, kb: KnowledgeBase, pivot: IndicatorPivot):
        self.kb = kb
        self.pivot = pivot
        self.sections: List[ReportSection] = []

    def generate_full_report(self) -> str:
        """生成完整的分析报告"""
        self.sections = []

        # 封面
        self._add_cover()

        # 按模块生成各章节
        for module in DIMENSION_REGISTRY.list_modules():
            section = self._generate_module_section(module)
            if section:
                self.sections.append(section)

        # 汇总与建议
        self._add_summary()

        # 组装
        lines = []
        for section in self.sections:
            lines.append(section.to_markdown())
            lines.append("")

        return "\n".join(lines)

    def _add_cover(self):
        """添加封面"""
        cover = ReportSection("医药行业TA效能研究报告", level=1)
        cover.add_paragraph(
            f"报告生成时间: {datetime.datetime.now().strftime('%Y年%m月%d日 %H:%M')}"
        )

        # 知识库统计
        stats = self.kb.get_statistics()
        cover.add_paragraph(
            f"本报告基于 {stats['total_entries']} 个分析指标自动生成，"
            f"覆盖 {len(stats['modules'])} 个分析模块。"
        )

        # 覆盖率
        kb_ids = list(self.kb.entries.keys())
        coverage_report = DIMENSION_REGISTRY.get_coverage_report(kb_ids)
        total_dims = len(DIMENSION_REGISTRY.get_available())
        covered_dims = sum(1 for d in DIMENSION_REGISTRY.get_available()
                          if d.entry_id in kb_ids)
        cover.add_paragraph(
            f"分析维度覆盖率: {covered_dims}/{total_dims} ({covered_dims/total_dims:.0%})"
            if total_dims > 0 else ""
        )

        self.sections.append(cover)

    def _generate_module_section(self, module: str) -> Optional[ReportSection]:
        """生成单个模块的报告章节"""
        dims = DIMENSION_REGISTRY.get_by_module(module)
        available_dims = [d for d in dims if d.is_available]

        if not available_dims:
            return None

        section = ReportSection(module, level=2)

        for dim_config in available_dims:
            entry = self.kb.get_entry(dim_config.entry_id)
            if entry is None:
                continue

            sub = ReportSection(dim_config.dimension, level=3)

            # 添加维度说明
            sub.add_paragraph(
                f"**分析维度**: {dim_config.definition}  \n"
                f"**计算公式**: {dim_config.formula}  \n"
                f"**数据来源**: {dim_config.data_source}"
            )

            # 添加数据表
            if isinstance(entry.metric_value, pd.DataFrame) and not entry.metric_value.empty:
                df_display = entry.metric_value.copy()
                # 格式化百分比列
                for col in df_display.columns:
                    if df_display[col].dtype in [np.float64, np.float32]:
                        max_val = df_display[col].max()
                        if 0 < max_val <= 1:
                            df_display[col] = df_display[col].apply(
                                lambda x: _fmt_pct(x) if pd.notna(x) else 'N/A'
                            )
                        else:
                            df_display[col] = df_display[col].apply(
                                lambda x: _fmt_num(x) if pd.notna(x) else 'N/A'
                            )
                sub.add_table(df_display, f'{dim_config.dimension} 数据表')

            # 添加透视分析发现
            pivot_result = self.pivot.results.get(dim_config.entry_id)
            if pivot_result and pivot_result.highlights:
                sub.add_paragraph("**关键发现:**")
                for h in pivot_result.highlights:
                    sub.add_bullet(h)

            section.add_sub_section(sub)

        return section if section.sub_sections else None

    def _add_summary(self):
        """添加汇总与建议"""
        summary = ReportSection("汇总与关键发现", level=2)

        all_highlights = self.pivot.get_all_highlights()
        if all_highlights:
            for entry_id, highlights in all_highlights.items():
                dim_config = DIMENSION_REGISTRY.get_by_entry_id(entry_id)
                if dim_config:
                    for h in highlights[:2]:  # 每个维度最多取2条
                        summary.add_bullet(f"[{dim_config.module}] {h}")
        else:
            summary.add_paragraph("暂无显著发现。")

        self.sections.append(summary)

    def export_structured_data(self) -> Dict:
        """导出结构化的报告数据（JSON格式）"""
        result = {
            'report_title': '医药行业TA效能研究报告',
            'generated_at': datetime.datetime.now().isoformat(),
            'modules': {},
        }

        for module in DIMENSION_REGISTRY.list_modules():
            module_data = {
                'dimensions': {},
            }

            for dim_config in DIMENSION_REGISTRY.get_by_module(module):
                if not dim_config.is_available:
                    continue

                entry = self.kb.get_entry(dim_config.entry_id)
                if entry is None:
                    continue

                dim_data = {
                    'dimension': dim_config.dimension,
                    'definition': dim_config.definition,
                    'formula': dim_config.formula,
                    'data_source': dim_config.data_source,
                }

                if isinstance(entry.metric_value, pd.DataFrame):
                    dim_data['data'] = entry.metric_value.to_dict('records')
                else:
                    dim_data['data'] = entry.metric_value

                pivot_result = self.pivot.results.get(dim_config.entry_id)
                if pivot_result:
                    dim_data['highlights'] = pivot_result.highlights
                    dim_data['rankings'] = pivot_result.rankings
                    dim_data['comparisons'] = pivot_result.comparisons

                module_data['dimensions'][dim_config.entry_id] = dim_data

            if module_data['dimensions']:
                result['modules'][module] = module_data

        return result
