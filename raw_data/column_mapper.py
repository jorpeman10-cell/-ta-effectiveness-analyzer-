"""
Column Mapper - 列名智能映射模块
将原始问卷中的各种列名变体映射到标准化的内部字段名
支持模糊匹配，解决不同年份/版本问卷列名不一致的问题
"""
import re
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher


class ColumnMapper:
    """列名标准化映射器"""

    # 标准字段定义: internal_name -> (中文标签, 匹配关键词列表)
    STANDARD_FIELDS = {
        # === 公司基本信息 ===
        'company': ('所属公司', ['所属公司', '公司名称', '公司']),
        'company_scale': ('公司规模', ['公司规模', '规模', 'scale']),

        # === 职能/职级维度 ===
        'function': ('职能', ['职能']),
        'level': ('职级', ['职级']),
        'sub_function': ('三级职能', ['三级职能', '二级职能', '细分职能']),

        # === 招聘量 ===
        'fte_new': ('FTE新增总量', ['FTE招聘总量-新增总量', 'FTE新增', '新增总量']),
        'fte_replace': ('FTE替换总量', ['FTE招聘总量-替换总量', 'FTE替换', '替换总量']),
        'fte_total': ('招聘总量', ['招聘总量', 'FTE招聘总量']),
        'contractor_total': ('三方员工招聘总量', ['三方员工招聘总量', '三方员工']),

        # === 招聘渠道（人数） ===
        'ch_hr_direct': ('HR直接招聘', ['HR直接招聘', 'HR直招']),
        'ch_external': ('外部渠道招聘', ['外部渠道招聘', '外部渠道']),
        'ch_headhunter': ('猎头', ['猎头 （单位：人）', '猎头（人）', '猎头']),
        'ch_rpo': ('RPO', ['RPO （单位：人）', 'RPO（人）']),
        'ch_referral': ('内部推荐', ['内部推荐 （单位：人）', '内部推荐', '内推（人）']),
        'ch_direct_apply': ('主动投递', ['主动投递 （单位：人）', '主动投递']),
        'ch_campus': ('校招', ['校招 （单位：人）', '校招']),
        'ch_internal_transfer': ('内部转岗', ['内部转岗 （单位：人数）', '内部转岗']),

        # === 招聘周期 ===
        'time_to_hire': ('招聘周期', ['招聘周期', 'offer发出', '单位：天']),

        # === 招聘成本 ===
        'cost_external_total': ('外部渠道费用成本', ['外部渠道费用成本', '外部渠道成本']),
        'cost_headhunter': ('猎头费', ['猎头费 （单位：万元）', '猎头费']),
        'cost_referral': ('内推费用', ['内推 （单位：万元）', '内推费用']),
        'cost_rpo': ('RPO费用', ['RPO （单位：万元）', 'RPO费用']),
        'cost_website': ('Website费用', ['Website费用', '招聘广告', 'linkedin']),
        'cost_other': ('其他费用', ['其他费用', 'Talent Mapping', '测评工具']),
        'cost_campus': ('校招成本', ['校招成本 （单位：万元）', '校招成本']),
        'cost_evp': ('EVP成本', ['EVP成本 （单位：万元）', 'EVP成本']),

        # === 人效 ===
        'productivity_total': ('人效招聘总量', ['人效招聘总量']),
        'hr_headcount': ('人力投入总量', ['人力投入总量']),
        'ta_productivity': ('TA生产率', ['TA生产率']),

        # === 招聘质量 ===
        'turnover_6m': ('6个月内离职率', ['6个月内离职率', '离职率']),
    }

    def __init__(self):
        self._cache: Dict[str, str] = {}

    def map_columns(self, raw_columns: List[str]) -> Dict[str, str]:
        """
        将原始列名列表映射到标准字段名
        返回: {原始列名: 标准字段名} 的映射字典
        """
        mapping = {}
        for raw_col in raw_columns:
            std_name = self._find_best_match(raw_col)
            if std_name:
                mapping[raw_col] = std_name
        return mapping

    def _find_best_match(self, raw_col: str) -> Optional[str]:
        """为原始列名找到最佳匹配的标准字段"""
        if raw_col in self._cache:
            return self._cache[raw_col]

        best_match = None
        best_score = 0.0

        for std_name, (label, keywords) in self.STANDARD_FIELDS.items():
            score = self._match_score(raw_col, keywords)
            if score > best_score and score >= 0.6:
                best_score = score
                best_match = std_name

        if best_match:
            self._cache[raw_col] = best_match
        return best_match

    def _match_score(self, raw_col: str, keywords: List[str]) -> float:
        """计算原始列名与关键词列表的匹配分数"""
        raw_clean = raw_col.strip().lower()
        max_score = 0.0

        for kw in keywords:
            kw_clean = kw.strip().lower()

            # 完全相等 - 最高优先级
            if raw_clean == kw_clean:
                return 1.0

            # 精确包含 - 优先更长的关键词匹配
            if kw_clean in raw_clean or raw_clean in kw_clean:
                # 长度越接近，分数越高（避免短关键词误匹配）
                len_ratio = min(len(kw_clean), len(raw_clean)) / max(len(kw_clean), len(raw_clean), 1)
                score = 0.85 + 0.14 * len_ratio
                max_score = max(max_score, min(score, 0.99))
                continue

            # 模糊匹配
            ratio = SequenceMatcher(None, raw_clean, kw_clean).ratio()
            max_score = max(max_score, ratio)

        return max_score

    def get_standard_label(self, std_name: str) -> str:
        """获取标准字段的中文标签"""
        if std_name in self.STANDARD_FIELDS:
            return self.STANDARD_FIELDS[std_name][0]
        return std_name

    def rename_dataframe(self, df, mapping: Dict[str, str] = None):
        """使用映射重命名DataFrame的列"""
        import pandas as pd
        if mapping is None:
            mapping = self.map_columns(df.columns.tolist())
        return df.rename(columns=mapping)

    def generate_mapping_report(self, raw_columns: List[str]) -> str:
        """生成列名映射报告，用于人工审核"""
        lines = ["# 列名映射报告", ""]
        lines.append(f"| 原始列名 | 标准字段 | 中文标签 | 匹配置信度 |")
        lines.append(f"|----------|----------|----------|------------|")

        for raw_col in raw_columns:
            std_name = self._find_best_match(raw_col)
            if std_name:
                label = self.get_standard_label(std_name)
                # 重新计算分数用于展示
                _, keywords = self.STANDARD_FIELDS[std_name]
                score = self._match_score(raw_col, keywords)
                lines.append(f"| {raw_col[:40]} | {std_name} | {label} | {score:.2f} |")
            else:
                lines.append(f"| {raw_col[:40]} | ❌ 未匹配 | - | - |")

        return "\n".join(lines)


if __name__ == '__main__':
    mapper = ColumnMapper()

    # 测试列名
    test_cols = [
        '所属公司', '公司规模', '职能', '职级',
        'FTE招聘总量-新增总量 （单位：人数）',
        '招聘总量',
        'HR直接招聘 （单位：人）',
        '猎头 （单位：人）',
        '招聘周期（从需求提出offer发出，不包含实习生） （单位：天）',
        '外部渠道费用成本（口径，包含猎头费、RPO费用、内推奖金等） （单位：万元）',
        'TA生产率',
    ]

    mapping = mapper.map_columns(test_cols)
    print("=== 映射结果 ===")
    for raw, std in mapping.items():
        print(f"  {raw[:50]:50s} -> {std}")

    print("\n=== 映射报告 ===")
    print(mapper.generate_mapping_report(test_cols))
