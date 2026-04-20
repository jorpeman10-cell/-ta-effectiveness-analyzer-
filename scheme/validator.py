"""
Validator - 数据验证与自查模块
确保报告的完整性和准确性
执行多层次验证: 数据完整性 → 计算准确性 → 逻辑一致性 → 覆盖率检查
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import datetime

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
from scheme.analysis_dimensions import DIMENSION_REGISTRY, DimensionConfig


@dataclass
class ValidationResult:
    """单项验证结果"""
    check_name: str
    status: str          # 'PASS', 'WARN', 'FAIL'
    message: str
    details: Any = None


@dataclass
class ValidationReport:
    """完整验证报告"""
    results: List[ValidationResult] = field(default_factory=list)
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.datetime.now().isoformat()

    @property
    def pass_count(self) -> int:
        return sum(1 for r in self.results if r.status == 'PASS')

    @property
    def warn_count(self) -> int:
        return sum(1 for r in self.results if r.status == 'WARN')

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if r.status == 'FAIL')

    @property
    def overall_status(self) -> str:
        if self.fail_count > 0:
            return 'FAIL'
        elif self.warn_count > 0:
            return 'WARN'
        return 'PASS'

    def to_markdown(self) -> str:
        lines = [
            "# 数据验证报告",
            f"\n验证时间: {self.timestamp}",
            f"\n**总体状态**: {self._status_icon(self.overall_status)} {self.overall_status}",
            f"  - ✅ 通过: {self.pass_count}",
            f"  - ⚠️ 警告: {self.warn_count}",
            f"  - ❌ 失败: {self.fail_count}",
            "",
            "## 详细结果",
            "",
            "| 状态 | 检查项 | 说明 |",
            "|------|--------|------|",
        ]

        for r in self.results:
            icon = self._status_icon(r.status)
            lines.append(f"| {icon} | {r.check_name} | {r.message} |")

        return "\n".join(lines)

    @staticmethod
    def _status_icon(status: str) -> str:
        return {'PASS': '✅', 'WARN': '⚠️', 'FAIL': '❌'}.get(status, '❓')


class Validator:
    """数据验证器"""

    def __init__(self, kb: KnowledgeBase):
        self.kb = kb
        self.report = ValidationReport()

    def validate_all(self) -> ValidationReport:
        """执行全部验证"""
        print("\n" + "=" * 60)
        print("  开始数据验证与自查")
        print("=" * 60)

        self._check_coverage()
        self._check_data_completeness()
        self._check_ratio_bounds()
        self._check_consistency()
        self._check_sample_size()

        print(f"\n验证完成: {self.report.overall_status}")
        print(f"  ✅ {self.report.pass_count} 通过")
        print(f"  ⚠️ {self.report.warn_count} 警告")
        print(f"  ❌ {self.report.fail_count} 失败")

        return self.report

    def _check_coverage(self):
        """检查分析维度覆盖率"""
        available = DIMENSION_REGISTRY.get_available()
        kb_ids = set(self.kb.entries.keys())

        covered = sum(1 for d in available if d.entry_id in kb_ids)
        total = len(available)
        ratio = covered / total if total > 0 else 0

        if ratio >= 0.9:
            self._add_result('维度覆盖率', 'PASS',
                             f'{covered}/{total} ({ratio:.0%}) 维度已覆盖')
        elif ratio >= 0.7:
            self._add_result('维度覆盖率', 'WARN',
                             f'{covered}/{total} ({ratio:.0%}) 维度已覆盖，部分维度缺失')
        else:
            self._add_result('维度覆盖率', 'FAIL',
                             f'{covered}/{total} ({ratio:.0%}) 维度已覆盖，覆盖率过低')

        # 列出缺失的维度
        missing = [d for d in available if d.entry_id not in kb_ids]
        if missing:
            missing_names = [d.dimension for d in missing]
            self._add_result('缺失维度', 'WARN',
                             f'缺失 {len(missing)} 个维度: {", ".join(missing_names[:5])}')

    def _check_data_completeness(self):
        """检查每个知识条目的数据完整性"""
        for entry_id, entry in self.kb.entries.items():
            if isinstance(entry.metric_value, pd.DataFrame):
                df = entry.metric_value
                if df.empty:
                    self._add_result(f'数据完整性-{entry_id}', 'FAIL',
                                     f'条目 {entry_id} 数据为空')
                    continue

                # 检查空值率
                null_rate = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
                if null_rate > 0.5:
                    self._add_result(f'数据完整性-{entry_id}', 'WARN',
                                     f'条目 {entry_id} 空值率 {null_rate:.1%}')
                elif null_rate > 0.8:
                    self._add_result(f'数据完整性-{entry_id}', 'FAIL',
                                     f'条目 {entry_id} 空值率过高 {null_rate:.1%}')

        # 总体通过
        self._add_result('数据完整性-总体', 'PASS', '所有条目数据完整性检查完成')

    def _check_ratio_bounds(self):
        """检查比率类指标是否在合理范围内 (0-1)"""
        ratio_entries = [
            e for e in self.kb.entries.values()
            if '占比' in e.metric_name or 'ratio' in e.metric_name.lower()
        ]

        issues = []
        for entry in ratio_entries:
            if isinstance(entry.metric_value, pd.DataFrame):
                df = entry.metric_value
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    if '占比' in col or 'P50' in col:
                        max_val = df[col].max()
                        min_val = df[col].min()
                        if pd.notna(max_val) and max_val > 1.05:
                            issues.append(f'{entry.entry_id}.{col}: max={max_val:.2f} > 1')
                        if pd.notna(min_val) and min_val < -0.05:
                            issues.append(f'{entry.entry_id}.{col}: min={min_val:.2f} < 0')

        if issues:
            self._add_result('比率范围检查', 'WARN',
                             f'{len(issues)} 个比率值超出 [0,1] 范围',
                             details=issues[:10])
        else:
            self._add_result('比率范围检查', 'PASS', '所有比率值在合理范围内')

    def _check_consistency(self):
        """检查逻辑一致性"""
        # 检查1: 各职能占比之和应接近100%
        vol_func = self.kb.get_entry('vol.func.overall')
        if vol_func and isinstance(vol_func.metric_value, pd.DataFrame):
            df = vol_func.metric_value
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                total = df[col].sum()
                if pd.notna(total) and abs(total - 1.0) > 0.15:
                    self._add_result('一致性-职能占比求和', 'WARN',
                                     f'各职能{col}之和为 {total:.2f}，偏离100%较大')
                else:
                    self._add_result('一致性-职能占比求和', 'PASS',
                                     f'各职能{col}之和为 {total:.2f}，在合理范围内')

        # 检查2: A类和B类公司的数据应该都存在
        for entry_id, entry in self.kb.entries.items():
            if isinstance(entry.metric_value, pd.DataFrame) and '公司规模' in entry.metric_value.columns:
                scales = entry.metric_value['公司规模'].unique()
                if 'A' not in scales or 'B' not in scales:
                    self._add_result(f'一致性-公司规模-{entry_id}', 'WARN',
                                     f'条目 {entry_id} 缺少A或B类公司数据 (现有: {list(scales)})')

    def _check_sample_size(self):
        """检查样本量是否足够"""
        for entry_id, entry in self.kb.entries.items():
            if isinstance(entry.metric_value, pd.DataFrame):
                df = entry.metric_value
                if len(df) < 2:
                    self._add_result(f'样本量-{entry_id}', 'WARN',
                                     f'条目 {entry_id} 仅有 {len(df)} 行数据，样本量可能不足')

        self._add_result('样本量-总体', 'PASS', '样本量检查完成')

    def _add_result(self, check_name: str, status: str, message: str, details=None):
        """添加验证结果"""
        self.report.results.append(ValidationResult(
            check_name=check_name,
            status=status,
            message=message,
            details=details,
        ))

    def get_report(self) -> ValidationReport:
        return self.report
