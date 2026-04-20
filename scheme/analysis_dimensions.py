"""
Analysis Dimensions - 分析维度配置
从Excel分析模型中提取的完整指标体系定义
每个维度定义了: 模块、类别、细分维度、定义、计算公式、数据源映射
"""
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class DimensionConfig:
    """单个分析维度的配置"""
    module: str              # 模块 e.g. "招聘量指标"
    category: str            # 类别 e.g. "一级数据"
    dimension: str           # 细分维度 e.g. "各职能招聘量占比"
    definition: str          # 定义 e.g. "BY不同职能"
    formula: str             # 计算公式
    data_source: str         # 数据源表 e.g. "4.2_职能"
    entry_id: str            # 对应Wiki知识库的entry_id
    group_by: List[str]      # 分组维度
    is_available: bool = True  # 是否有数据可计算
    notes: str = ""


class AnalysisDimensionRegistry:
    """
    分析维度注册表 - 完整的指标体系
    基于 '招聘指标分析 Skill.xlsx' 中的分析维度表
    """

    def __init__(self):
        self.dimensions: List[DimensionConfig] = []
        self._build_registry()

    def _build_registry(self):
        """构建完整的分析维度注册表"""

        # ==================== 招聘量指标 ====================
        self._add('招聘量指标', '一级数据', '各职能招聘量占比',
                   'BY不同职能', '各职能招聘数/FTE招聘总数',
                   '4.2_职能', 'vol.func.overall', ['职能'])

        self._add('招聘量指标', '一级数据', '各职能下不同规模公司招聘量占比',
                   'BY不同职能、不同规模公司', '各职能招聘数/FTE招聘总数',
                   '4.2_职能', 'vol.func.by_scale', ['职能', '公司规模'])

        self._add('招聘量指标', '一级数据', '各职级招聘量占比',
                   'BY不同职级', '各职级招聘数/FTE招聘总数',
                   '4.1_职级', 'vol.level.overall', ['职级'])

        self._add('招聘量指标', '一级数据', '各职级下不同规模公司招聘量占比',
                   'BY不同职级、不同规模公司', '各职级招聘数/FTE招聘总数',
                   '4.1_职级', 'vol.level.by_scale', ['职级', '公司规模'])

        # ==================== 招聘渠道指标 ====================
        self._add('招聘渠道指标', '一级数据', '各招聘渠道招聘量占比',
                   'BY不同招聘渠道、不同规模公司', '不同渠道招聘数/FTE招聘总数',
                   '4.2_职能', 'channel.overall.by_scale', ['招聘渠道', '公司规模'])

        self._add('招聘渠道指标', '一级数据', '各职能下不同招聘渠道招聘量占比',
                   'BY不同职能、不同招聘渠道', '各职能不同渠道招聘数/各职能FTE招聘总数',
                   '4.2_职能', 'channel.func.overall', ['职能', '招聘渠道'])

        self._add('招聘渠道指标', '一级数据', '不同规模公司不同招聘渠道招聘量占比',
                   'BY不同招聘渠道、不同规模公司', '不同渠道招聘数/FTE招聘总数',
                   '4.2_职能', 'channel.overall.by_scale', ['招聘渠道', '公司规模'])

        self._add('招聘渠道指标', '一级数据', '各职级下不同招聘渠道招聘量占比',
                   'BY不同职级、不同招聘渠道', '各职级不同渠道招聘数/各职级FTE招聘总数',
                   '4.1_职级', 'channel.level.overall', ['职级', '招聘渠道'])

        # ==================== 招聘周期指标 ====================
        self._add('招聘周期指标', '一级数据', '各职能招聘周期',
                   'BY 不同职能', '直接数据',
                   '4.2_职能', 'tth.func.overall', ['职能'])

        self._add('招聘周期指标', '一级交叉分析', '各职能下不同规模公司招聘周期',
                   'BY不同职能、不同规模公司', '直接数据',
                   '4.2_职能', 'tth.func.by_scale', ['职能', '公司规模'])

        self._add('招聘周期指标', '一级数据', '各职级招聘周期',
                   'BY 不同职级', '直接数据',
                   '4.1_职级', 'tth.level.overall', ['职级'])

        self._add('招聘周期指标', '一级交叉分析', '各职级下不同规模公司招聘周期',
                   'BY不同职级、不同规模公司', '直接数据',
                   '4.1_职级', 'tth.level.by_scale', ['职级', '公司规模'])

        # ==================== 招聘成本指标 ====================
        self._add('招聘成本指标', '一级直接数据', '不同规模公司不同招聘渠道成本占比',
                   'BY不同规模公司、不同招聘渠道', '各招聘渠道成本额/总成本额',
                   '4.2_职能', 'cost.channel.by_scale', ['公司规模', '招聘渠道'])

        self._add('招聘成本指标', '一级直接数据', '不同职能招聘成本占比',
                   'BY不同职能', '各职能招聘成本额/总成本额',
                   '4.2_职能', 'cost.func.ratio', ['职能'])

        self._add('招聘成本指标', '一级直接数据', '不同职能单个职位招聘成本',
                   'BY不同职能', '各职能招聘成本/各职能FTE招聘总数',
                   '4.2_职能', 'cost.func.per_hire', ['职能'])

        self._add('招聘成本指标', '一级直接数据', '不同规模公司不同职能人均招聘成本',
                   'BY不同职能、不同规模公司', '各职能招聘成本/各职能FTE招聘总数',
                   '4.2_职能', 'cost.func.per_hire.by_scale', ['职能', '公司规模'])

        self._add('招聘成本指标', '没有数据', '不同职级单个职位招聘成本',
                   'BY不同职级', '各职级招聘成本/各职级FTE招聘总数',
                   '4.1_职级', 'cost.level.per_hire', ['职级'],
                   is_available=False, notes='没有数据，做不了')

        self._add('招聘成本指标', '没有数据', '不同规模公司不同职级人均招聘成本',
                   'BY不同职级、不同规模公司', '',
                   '4.1_职级', 'cost.level.per_hire.by_scale', ['职级', '公司规模'],
                   is_available=False, notes='没有数据，做不了')

        # ==================== 招聘质量指标 ====================
        self._add('招聘质量指标', '一级数据', '不同规模公司6个月内离职率平均值',
                   'BY整体、不同规模公司', '各规模公司离职率总和/各规模公司样本数',
                   '4.2_职能', 'quality.turnover_6m', ['公司规模'])

        # ==================== 细分职能报告-商业 ====================
        self._add('细分职能报告-商业', '二级数据', '商业-各二级职能渠道招聘量占比',
                   'BY不同商业职能、不同招聘渠道',
                   '各商业职能不同渠道招聘数/各商业职能FTE招聘总数',
                   '4.4_商业', 'commercial.channel.sub_func', ['商业二级职能', '招聘渠道'])

        self._add('细分职能报告-商业', '二级数据', '商业-各二级职能招聘量占比',
                   'BY不同商业职能', '各商业职能招聘数/FTE招聘总数',
                   '4.4_商业', 'commercial.vol.sub_func', ['商业二级职能'])

        self._add('细分职能报告-商业', '二级数据', '商业-各二级职能招聘周期',
                   'BY不同商业职能', '直接数据',
                   '4.4_商业', 'commercial.tth.sub_func', ['商业二级职能'])

        # ==================== 细分职能报告-研发 ====================
        self._add('细分职能报告-研发', '二级数据', '研发-各二级职能渠道招聘量占比',
                   'BY不同研发职能、不同招聘渠道',
                   '各研发职能不同渠道招聘数/各研发职能FTE招聘总数',
                   '4.3_研发', 'rd.channel.sub_func', ['研发二级职能', '招聘渠道'])

        self._add('细分职能报告-研发', '二级数据', '研发-各二级职能招聘量占比',
                   'BY不同研发职能', '各研发职能招聘数/FTE招聘总数',
                   '4.3_研发', 'rd.vol.sub_func', ['研发二级职能'])

        self._add('细分职能报告-研发', '二级数据', '研发-各二级职能招聘周期',
                   'BY不同研发职能', '直接数据',
                   '4.3_研发', 'rd.tth.sub_func', ['研发二级职能'])

        # ==================== TA生产率 ====================
        self._add('TA生产率', '一级数据', '不同规模公司TA生产率',
                   'BY不同规模公司', '人效招聘总量/人力投入总量',
                   '4.2_职能', 'productivity.by_scale', ['公司规模'])

    def _add(self, module, category, dimension, definition, formula,
             data_source, entry_id, group_by, is_available=True, notes=''):
        self.dimensions.append(DimensionConfig(
            module=module,
            category=category,
            dimension=dimension,
            definition=definition,
            formula=formula,
            data_source=data_source,
            entry_id=entry_id,
            group_by=group_by,
            is_available=is_available,
            notes=notes,
        ))

    def get_all(self) -> List[DimensionConfig]:
        return self.dimensions

    def get_available(self) -> List[DimensionConfig]:
        """获取所有可计算的维度"""
        return [d for d in self.dimensions if d.is_available]

    def get_by_module(self, module: str) -> List[DimensionConfig]:
        return [d for d in self.dimensions if d.module == module]

    def get_by_entry_id(self, entry_id: str) -> Optional[DimensionConfig]:
        for d in self.dimensions:
            if d.entry_id == entry_id:
                return d
        return None

    def list_modules(self) -> List[str]:
        seen = []
        for d in self.dimensions:
            if d.module not in seen:
                seen.append(d.module)
        return seen

    def get_coverage_report(self, kb_entry_ids: List[str]) -> str:
        """生成维度覆盖率报告"""
        lines = ["# 分析维度覆盖率报告", ""]
        total = len(self.get_available())
        covered = sum(1 for d in self.get_available() if d.entry_id in kb_entry_ids)

        lines.append(f"**总维度数**: {len(self.dimensions)}")
        lines.append(f"**可计算维度**: {total}")
        lines.append(f"**已覆盖维度**: {covered}")
        lines.append(f"**覆盖率**: {covered/total:.1%}" if total > 0 else "**覆盖率**: N/A")
        lines.append("")

        for module in self.list_modules():
            dims = self.get_by_module(module)
            lines.append(f"\n## {module}")
            lines.append("")
            lines.append("| 维度 | 类别 | 状态 | Entry ID |")
            lines.append("|------|------|------|----------|")
            for d in dims:
                if not d.is_available:
                    status = "❌ 无数据"
                elif d.entry_id in kb_entry_ids:
                    status = "✅ 已覆盖"
                else:
                    status = "⚠️ 未计算"
                lines.append(f"| {d.dimension} | {d.category} | {status} | {d.entry_id} |")

        return "\n".join(lines)

    def to_dict_list(self) -> List[Dict]:
        """导出为字典列表"""
        return [
            {
                'module': d.module,
                'category': d.category,
                'dimension': d.dimension,
                'definition': d.definition,
                'formula': d.formula,
                'data_source': d.data_source,
                'entry_id': d.entry_id,
                'group_by': d.group_by,
                'is_available': d.is_available,
                'notes': d.notes,
            }
            for d in self.dimensions
        ]


# 全局单例
DIMENSION_REGISTRY = AnalysisDimensionRegistry()


if __name__ == '__main__':
    reg = DIMENSION_REGISTRY
    print(f"总维度数: {len(reg.get_all())}")
    print(f"可计算维度: {len(reg.get_available())}")
    print(f"\n模块列表:")
    for m in reg.list_modules():
        dims = reg.get_by_module(m)
        avail = sum(1 for d in dims if d.is_available)
        print(f"  {m}: {len(dims)}个维度 ({avail}个可计算)")
