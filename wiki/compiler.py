"""
Data Compiler - 数据编译器
将Raw Data编译为Wiki知识条目
核心职责: 从原始DataFrame中按分析维度配置自动摘取数据、计算指标、生成KnowledgeEntry
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from .knowledge_base import KnowledgeBase, KnowledgeEntry


class DataCompiler:
    """数据编译器 - 将原始数据编译为知识库条目"""

    def __init__(self):
        self.kb = KnowledgeBase()
        self._compile_errors: List[Dict] = []

    def compile_all(self, raw_datasets: Dict[str, Any]) -> KnowledgeBase:
        """
        编译所有原始数据集为知识库
        raw_datasets: {key: RawDataset} 从 ingestor.ingest_questionnaire_set() 获取
        """
        print("\n" + "=" * 60)
        print("  开始编译原始数据 → Wiki知识库")
        print("=" * 60)

        # 编译各模块
        if '4.2_职能' in raw_datasets:
            self._compile_recruitment_volume_by_function(raw_datasets['4.2_职能'])
            self._compile_channel_by_function(raw_datasets['4.2_职能'])
            self._compile_time_to_hire_by_function(raw_datasets['4.2_职能'])
            self._compile_cost_by_function(raw_datasets['4.2_职能'])
            self._compile_ta_productivity(raw_datasets['4.2_职能'])

        if '4.1_职级' in raw_datasets:
            self._compile_recruitment_volume_by_level(raw_datasets['4.1_职级'])
            self._compile_channel_by_level(raw_datasets['4.1_职级'])
            self._compile_time_to_hire_by_level(raw_datasets['4.1_职级'])
            self._compile_cost_by_level(raw_datasets['4.1_职级'])

        if '4.4_商业' in raw_datasets:
            self._compile_commercial_breakdown(raw_datasets['4.4_商业'])

        if '4.3_研发' in raw_datasets:
            self._compile_rd_breakdown(raw_datasets['4.3_研发'])

        if '4.7_问卷' in raw_datasets:
            self._compile_quality_metrics(raw_datasets)

        # 汇总
        stats = self.kb.get_statistics()
        print(f"\n[OK] 编译完成: {stats['total_entries']} 条知识条目")
        print(f"   模块: {', '.join(stats['modules'])}")
        if self._compile_errors:
            print(f"   [WARN] {len(self._compile_errors)} 个编译警告")

        return self.kb

    # ==================== 招聘量指标 ====================

    def _compile_recruitment_volume_by_function(self, dataset):
        """编译: 各职能招聘量占比 (BY 职能, BY 职能×公司规模)"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        # 获取公司整体行作为分母
        total_col = self._find_col(df, ['招聘总量'])
        func_col = self._find_col(df, ['职能'])
        company_col = self._find_col(df, ['所属公司'])
        scale_col = self._find_col(df, ['公司规模'])

        if not all([total_col, func_col, company_col]):
            self._log_error('recruitment_volume_by_function', '缺少必要列')
            return

        # 获取公司整体招聘总量
        company_total = df[df[func_col] == '公司整体'][[company_col, total_col]].copy()
        company_total = company_total.rename(columns={total_col: '_company_total'})

        # 各职能明细
        detail = df[df[func_col] != '公司整体'].copy()
        detail = detail.merge(company_total, on=company_col, how='left')
        detail['_ratio'] = np.where(
            detail['_company_total'].notna() & (detail['_company_total'] != 0),
            detail[total_col] / detail['_company_total'],
            np.nan
        )

        # 1) 各职能招聘量占比 (整体P50)
        overall = detail.groupby(func_col)['_ratio'].median().reset_index()
        overall.columns = ['职能', '招聘量占比_P50']
        self.kb.add_entry(KnowledgeEntry(
            entry_id='vol.func.overall',
            module='招聘量指标',
            dimension='各职能招聘量占比',
            group_by=['职能'],
            metric_name='招聘量占比_P50',
            metric_value=overall,
            formula='各职能招聘数/FTE招聘总数 (P50)',
            data_source='4.2_职能',
            tags=['招聘量', '职能', '一级数据'],
        ))

        # 2) 各职能×公司规模 招聘量占比
        if scale_col:
            by_scale = detail.groupby([scale_col, func_col])['_ratio'].median().reset_index()
            by_scale.columns = ['公司规模', '职能', '招聘量占比_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='vol.func.by_scale',
                module='招聘量指标',
                dimension='各职能下不同规模公司招聘量占比',
                group_by=['职能', '公司规模'],
                metric_name='招聘量占比_P50',
                metric_value=by_scale,
                formula='各职能招聘数/FTE招聘总数 (按公司规模分组P50)',
                data_source='4.2_职能',
                tags=['招聘量', '职能', '公司规模', '一级数据'],
            ))

        print("  [OK] 招聘量指标 - 职能维度")

    def _compile_recruitment_volume_by_level(self, dataset):
        """编译: 各职级招聘量占比"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        total_col = self._find_col(df, ['招聘总量'])
        level_col = self._find_col(df, ['职级'])
        company_col = self._find_col(df, ['所属公司'])
        scale_col = self._find_col(df, ['公司规模'])

        if not all([total_col, level_col, company_col]):
            self._log_error('recruitment_volume_by_level', '缺少必要列')
            return

        company_total = df[df[level_col] == '公司整体'][[company_col, total_col]].copy()
        company_total = company_total.rename(columns={total_col: '_company_total'})

        detail = df[df[level_col] != '公司整体'].copy()
        detail = detail.merge(company_total, on=company_col, how='left')
        detail['_ratio'] = np.where(
            detail['_company_total'].notna() & (detail['_company_total'] != 0),
            detail[total_col] / detail['_company_total'],
            np.nan
        )

        # 各职级招聘量占比
        overall = detail.groupby(level_col)['_ratio'].median().reset_index()
        overall.columns = ['职级', '招聘量占比_P50']
        self.kb.add_entry(KnowledgeEntry(
            entry_id='vol.level.overall',
            module='招聘量指标',
            dimension='各职级招聘量占比',
            group_by=['职级'],
            metric_name='招聘量占比_P50',
            metric_value=overall,
            formula='各职级招聘数/FTE招聘总数 (P50)',
            data_source='4.1_职级',
            tags=['招聘量', '职级', '一级数据'],
        ))

        if scale_col:
            by_scale = detail.groupby([scale_col, level_col])['_ratio'].median().reset_index()
            by_scale.columns = ['公司规模', '职级', '招聘量占比_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='vol.level.by_scale',
                module='招聘量指标',
                dimension='各职级下不同规模公司招聘量占比',
                group_by=['职级', '公司规模'],
                metric_name='招聘量占比_P50',
                metric_value=by_scale,
                formula='各职级招聘数/FTE招聘总数 (按公司规模分组P50)',
                data_source='4.1_职级',
                tags=['招聘量', '职级', '公司规模', '一级数据'],
            ))

        print("  [OK] 招聘量指标 - 职级维度")

    # ==================== 招聘渠道指标 ====================

    def _compile_channel_by_function(self, dataset):
        """编译: 各职能下不同招聘渠道招聘量占比"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        total_col = self._find_col(df, ['招聘总量'])
        func_col = self._find_col(df, ['职能'])
        scale_col = self._find_col(df, ['公司规模'])
        ext_col = self._find_col(df, ['外部渠道招聘'])

        channel_cols = {
            'HR直招': self._find_col(df, ['HR直接招聘']),
            '猎头': self._find_col(df, ['猎头 （单位：人）', '猎头']),
            'RPO': self._find_col(df, ['RPO （单位：人）']),
            '内部推荐': self._find_col(df, ['内部推荐']),
            '主动投递': self._find_col(df, ['主动投递']),
            '校招': self._find_col(df, ['校招 （单位：人）', '校招']),
            '内部转岗': self._find_col(df, ['内部转岗']),
        }

        # 公司整体行 - 各渠道占总招聘量比例
        df_company = df[df[func_col] == '公司整体'].copy()

        results = []
        for ch_name, ch_col in channel_cols.items():
            if ch_col is None or total_col is None:
                continue
            df_company[f'_{ch_name}_ratio'] = np.where(
                df_company[total_col].notna() & (df_company[total_col] != 0),
                df_company[ch_col] / df_company[total_col],
                np.nan
            )

        # 按公司规模计算P50
        if scale_col:
            for ch_name, ch_col in channel_cols.items():
                ratio_col = f'_{ch_name}_ratio'
                if ratio_col in df_company.columns:
                    p50 = df_company.groupby(scale_col)[ratio_col].median().reset_index()
                    p50.columns = ['公司规模', 'P50值']
                    p50['渠道'] = ch_name
                    results.append(p50)

        if results:
            all_channels = pd.concat(results, ignore_index=True)
            self.kb.add_entry(KnowledgeEntry(
                entry_id='channel.overall.by_scale',
                module='招聘渠道指标',
                dimension='各招聘渠道招聘量占比',
                group_by=['招聘渠道', '公司规模'],
                metric_name='渠道占比_P50',
                metric_value=all_channels,
                formula='不同渠道招聘数/FTE招聘总数 (P50)',
                data_source='4.2_职能',
                tags=['渠道', '公司规模', '一级数据'],
            ))

        # 各职能下的渠道分布
        detail = df[df[func_col] != '公司整体'].copy()
        for ch_name, ch_col in channel_cols.items():
            if ch_col and total_col:
                detail[f'_{ch_name}_ratio'] = np.where(
                    detail[total_col].notna() & (detail[total_col] != 0),
                    detail[ch_col] / detail[total_col],
                    np.nan
                )

        func_channel_results = []
        for ch_name in channel_cols:
            ratio_col = f'_{ch_name}_ratio'
            if ratio_col in detail.columns and func_col:
                p50 = detail.groupby(func_col)[ratio_col].median().reset_index()
                p50.columns = ['职能', 'P50值']
                p50['渠道'] = ch_name
                func_channel_results.append(p50)

        if func_channel_results:
            func_channels = pd.concat(func_channel_results, ignore_index=True)
            self.kb.add_entry(KnowledgeEntry(
                entry_id='channel.func.overall',
                module='招聘渠道指标',
                dimension='各职能下不同招聘渠道招聘量占比',
                group_by=['职能', '招聘渠道'],
                metric_name='渠道占比_P50',
                metric_value=func_channels,
                formula='各职能不同渠道招聘数/各职能FTE招聘总数 (P50)',
                data_source='4.2_职能',
                tags=['渠道', '职能', '一级数据'],
            ))

        print("  [OK] 招聘渠道指标 - 职能维度")

    def _compile_channel_by_level(self, dataset):
        """编译: 各职级下不同招聘渠道招聘量占比"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        total_col = self._find_col(df, ['招聘总量'])
        level_col = self._find_col(df, ['职级'])
        scale_col = self._find_col(df, ['公司规模'])

        channel_cols = {
            '猎头': self._find_col(df, ['猎头 （单位：人）', '猎头']),
            'RPO': self._find_col(df, ['RPO （单位：人）']),
            '内部推荐': self._find_col(df, ['内部推荐']),
            '主动投递': self._find_col(df, ['主动投递']),
            '校招': self._find_col(df, ['校招 （单位：人）', '校招']),
        }

        detail = df[df[level_col] != '公司整体'].copy()
        for ch_name, ch_col in channel_cols.items():
            if ch_col and total_col:
                detail[f'_{ch_name}_ratio'] = np.where(
                    detail[total_col].notna() & (detail[total_col] != 0),
                    detail[ch_col] / detail[total_col],
                    np.nan
                )

        results = []
        for ch_name in channel_cols:
            ratio_col = f'_{ch_name}_ratio'
            if ratio_col in detail.columns and level_col:
                p50 = detail.groupby(level_col)[ratio_col].median().reset_index()
                p50.columns = ['职级', 'P50值']
                p50['渠道'] = ch_name
                results.append(p50)

        if results:
            all_data = pd.concat(results, ignore_index=True)
            self.kb.add_entry(KnowledgeEntry(
                entry_id='channel.level.overall',
                module='招聘渠道指标',
                dimension='各职级下不同招聘渠道招聘量占比',
                group_by=['职级', '招聘渠道'],
                metric_name='渠道占比_P50',
                metric_value=all_data,
                formula='各职级不同渠道招聘数/各职级FTE招聘总数 (P50)',
                data_source='4.1_职级',
                tags=['渠道', '职级', '一级数据'],
            ))

        print("  [OK] 招聘渠道指标 - 职级维度")

    # ==================== 招聘周期指标 ====================

    def _compile_time_to_hire_by_function(self, dataset):
        """编译: 各职能招聘周期"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        func_col = self._find_col(df, ['职能'])
        scale_col = self._find_col(df, ['公司规模'])
        tth_col = self._find_col(df, ['招聘周期'])

        if not all([func_col, tth_col]):
            self._log_error('time_to_hire_by_function', '缺少必要列')
            return

        detail = df[df[func_col] != '公司整体'].copy()

        # 各职能招聘周期P50
        overall = detail.groupby(func_col)[tth_col].median().reset_index()
        overall.columns = ['职能', '招聘周期_P50']
        self.kb.add_entry(KnowledgeEntry(
            entry_id='tth.func.overall',
            module='招聘周期指标',
            dimension='各职能招聘周期',
            group_by=['职能'],
            metric_name='招聘周期_P50',
            metric_value=overall,
            formula='直接数据 (P50)',
            data_source='4.2_职能',
            tags=['招聘周期', '职能', '一级数据'],
        ))

        # 各职能×公司规模
        if scale_col:
            by_scale = detail.groupby([scale_col, func_col])[tth_col].median().reset_index()
            by_scale.columns = ['公司规模', '职能', '招聘周期_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='tth.func.by_scale',
                module='招聘周期指标',
                dimension='各职能下不同规模公司招聘周期',
                group_by=['职能', '公司规模'],
                metric_name='招聘周期_P50',
                metric_value=by_scale,
                formula='直接数据 (按公司规模分组P50)',
                data_source='4.2_职能',
                tags=['招聘周期', '职能', '公司规模', '一级交叉分析'],
            ))

        print("  [OK] 招聘周期指标 - 职能维度")

    def _compile_time_to_hire_by_level(self, dataset):
        """编译: 各职级招聘周期"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        level_col = self._find_col(df, ['职级'])
        scale_col = self._find_col(df, ['公司规模'])
        tth_col = self._find_col(df, ['招聘周期'])

        if not all([level_col, tth_col]):
            self._log_error('time_to_hire_by_level', '缺少必要列')
            return

        detail = df[df[level_col] != '公司整体'].copy()

        overall = detail.groupby(level_col)[tth_col].median().reset_index()
        overall.columns = ['职级', '招聘周期_P50']
        self.kb.add_entry(KnowledgeEntry(
            entry_id='tth.level.overall',
            module='招聘周期指标',
            dimension='各职级招聘周期',
            group_by=['职级'],
            metric_name='招聘周期_P50',
            metric_value=overall,
            formula='直接数据 (P50)',
            data_source='4.1_职级',
            tags=['招聘周期', '职级', '一级数据'],
        ))

        if scale_col:
            by_scale = detail.groupby([scale_col, level_col])[tth_col].median().reset_index()
            by_scale.columns = ['公司规模', '职级', '招聘周期_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='tth.level.by_scale',
                module='招聘周期指标',
                dimension='各职级下不同规模公司招聘周期',
                group_by=['职级', '公司规模'],
                metric_name='招聘周期_P50',
                metric_value=by_scale,
                formula='直接数据 (按公司规模分组P50)',
                data_source='4.1_职级',
                tags=['招聘周期', '职级', '公司规模', '一级交叉分析'],
            ))

        print("  [OK] 招聘周期指标 - 职级维度")

    # ==================== 招聘成本指标 ====================

    def _compile_cost_by_function(self, dataset):
        """编译: 不同职能招聘成本占比"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        func_col = self._find_col(df, ['职能'])
        scale_col = self._find_col(df, ['公司规模'])
        company_col = self._find_col(df, ['所属公司'])
        total_col = self._find_col(df, ['招聘总量'])
        cost_ext_col = self._find_col(df, ['外部渠道费用成本', '外部渠道成本'])

        cost_channels = {
            '猎头费': self._find_col(df, ['猎头费']),
            '内推费用': self._find_col(df, ['内推 （单位：万元）', '内推费用']),
            'RPO费用': self._find_col(df, ['RPO （单位：万元）', 'RPO费用']),
        }

        if not func_col or not cost_ext_col:
            self._log_error('cost_by_function', '缺少必要列')
            return

        # 公司整体成本
        company_total_cost = df[df[func_col] == '公司整体'][[company_col, cost_ext_col]].copy()
        company_total_cost = company_total_cost.rename(columns={cost_ext_col: '_total_cost'})

        detail = df[df[func_col] != '公司整体'].copy()
        detail = detail.merge(company_total_cost, on=company_col, how='left')

        # 各职能成本占比
        detail['_cost_ratio'] = np.where(
            detail['_total_cost'].notna() & (detail['_total_cost'] != 0),
            detail[cost_ext_col] / detail['_total_cost'],
            np.nan
        )

        overall = detail.groupby(func_col)['_cost_ratio'].median().reset_index()
        overall.columns = ['职能', '成本占比_P50']
        self.kb.add_entry(KnowledgeEntry(
            entry_id='cost.func.ratio',
            module='招聘成本指标',
            dimension='不同职能招聘成本占比',
            group_by=['职能'],
            metric_name='成本占比_P50',
            metric_value=overall,
            formula='各职能招聘成本额/总成本额 (P50)',
            data_source='4.2_职能',
            tags=['成本', '职能', '一级直接数据'],
        ))

        # 各职能单个职位招聘成本
        if total_col:
            detail['_cost_per_hire'] = np.where(
                detail[total_col].notna() & (detail[total_col] != 0),
                detail[cost_ext_col] / detail[total_col],
                np.nan
            )
            per_hire = detail.groupby(func_col)['_cost_per_hire'].median().reset_index()
            per_hire.columns = ['职能', '单位招聘成本_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='cost.func.per_hire',
                module='招聘成本指标',
                dimension='不同职能单个职位招聘成本',
                group_by=['职能'],
                metric_name='单位招聘成本_P50',
                metric_value=per_hire,
                formula='各职能招聘成本/各职能FTE招聘总数 (P50)',
                data_source='4.2_职能',
                tags=['成本', '职能', '人均成本'],
            ))

            # 按公司规模
            if scale_col:
                per_hire_scale = detail.groupby([scale_col, func_col])['_cost_per_hire'].median().reset_index()
                per_hire_scale.columns = ['公司规模', '职能', '单位招聘成本_P50']
                self.kb.add_entry(KnowledgeEntry(
                    entry_id='cost.func.per_hire.by_scale',
                    module='招聘成本指标',
                    dimension='不同规模公司不同职能人均招聘成本',
                    group_by=['职能', '公司规模'],
                    metric_name='单位招聘成本_P50',
                    metric_value=per_hire_scale,
                    formula='各职能招聘成本/各职能FTE招聘总数 (按公司规模P50)',
                    data_source='4.2_职能',
                    tags=['成本', '职能', '公司规模', '人均成本'],
                ))

        # 各渠道成本占比
        for ch_name, ch_col in cost_channels.items():
            if ch_col and cost_ext_col:
                detail[f'_{ch_name}_cost_ratio'] = np.where(
                    detail[cost_ext_col].notna() & (detail[cost_ext_col] != 0),
                    detail[ch_col] / detail[cost_ext_col],
                    np.nan
                )

        if scale_col:
            cost_channel_results = []
            for ch_name in cost_channels:
                ratio_col = f'_{ch_name}_cost_ratio'
                if ratio_col in detail.columns:
                    p50 = detail.groupby(scale_col)[ratio_col].median().reset_index()
                    p50.columns = ['公司规模', 'P50值']
                    p50['渠道'] = ch_name
                    cost_channel_results.append(p50)

            if cost_channel_results:
                all_cost_ch = pd.concat(cost_channel_results, ignore_index=True)
                self.kb.add_entry(KnowledgeEntry(
                    entry_id='cost.channel.by_scale',
                    module='招聘成本指标',
                    dimension='不同规模公司不同招聘渠道成本占比',
                    group_by=['招聘渠道', '公司规模'],
                    metric_name='渠道成本占比_P50',
                    metric_value=all_cost_ch,
                    formula='各招聘渠道成本额/总成本额 (P50)',
                    data_source='4.2_职能',
                    tags=['成本', '渠道', '公司规模', '一级直接数据'],
                ))

        print("  [OK] 招聘成本指标 - 职能维度")

    def _compile_cost_by_level(self, dataset):
        """编译: 不同职级招聘成本"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        level_col = self._find_col(df, ['职级'])
        scale_col = self._find_col(df, ['公司规模'])
        cost_cols = {
            '猎头费': self._find_col(df, ['猎头费']),
            '内推费用': self._find_col(df, ['内推 （单位：万元）', '内推费用']),
            'RPO费用': self._find_col(df, ['RPO （单位：万元）', 'RPO费用']),
        }

        if not level_col:
            return

        detail = df[df[level_col] != '公司整体'].copy()

        # 计算各渠道成本占总外部成本比例
        valid_cost_cols = [c for c in cost_cols.values() if c is not None]
        if valid_cost_cols:
            detail['_total_ext_cost'] = detail[valid_cost_cols].sum(axis=1, min_count=1)

            for ch_name, ch_col in cost_cols.items():
                if ch_col:
                    detail[f'_{ch_name}_ratio'] = np.where(
                        detail['_total_ext_cost'].notna() & (detail['_total_ext_cost'] != 0),
                        detail[ch_col] / detail['_total_ext_cost'],
                        np.nan
                    )

            results = []
            group_cols = [scale_col, level_col] if scale_col else [level_col]
            for ch_name in cost_cols:
                ratio_col = f'_{ch_name}_ratio'
                if ratio_col in detail.columns:
                    p50 = detail.groupby(group_cols)[ratio_col].median().reset_index()
                    p50['渠道'] = ch_name
                    results.append(p50)

            if results:
                all_data = pd.concat(results, ignore_index=True)
                self.kb.add_entry(KnowledgeEntry(
                    entry_id='cost.level.channel_ratio',
                    module='招聘成本指标',
                    dimension='不同职级不同招聘渠道成本占比',
                    group_by=['职级', '公司规模', '招聘渠道'],
                    metric_name='渠道成本占比_P50',
                    metric_value=all_data,
                    formula='各渠道成本/总外部渠道成本 (P50)',
                    data_source='4.1_职级',
                    tags=['成本', '职级', '渠道'],
                ))

        print("  [OK] 招聘成本指标 - 职级维度")

    # ==================== 招聘质量指标 ====================

    def _compile_quality_metrics(self, all_datasets):
        """编译: 招聘质量指标 (6个月内离职率)"""
        # 尝试从4.2中获取离职率数据
        dataset = all_datasets.get('4.2_职能')
        if dataset is None:
            return

        df = self._get_main_df(dataset)
        if df is None:
            return

        turnover_col = self._find_col(df, ['离职率', '6个月'])
        scale_col = self._find_col(df, ['公司规模'])
        func_col = self._find_col(df, ['职能'])

        if turnover_col and scale_col:
            company_rows = df[df[func_col] == '公司整体'].copy() if func_col else df.copy()
            by_scale = company_rows.groupby(scale_col)[turnover_col].mean().reset_index()
            by_scale.columns = ['公司规模', '6个月内离职率_均值']

            self.kb.add_entry(KnowledgeEntry(
                entry_id='quality.turnover_6m',
                module='招聘质量指标',
                dimension='不同规模公司6个月内离职率平均值',
                group_by=['公司规模'],
                metric_name='6个月内离职率_均值',
                metric_value=by_scale,
                formula='各规模公司离职率总和/各规模公司样本数',
                data_source='4.2_职能',
                tags=['质量', '离职率', '公司规模'],
            ))
            print("  [OK] 招聘质量指标")

    # ==================== TA生产率 ====================

    def _compile_ta_productivity(self, dataset):
        """编译: TA生产率"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        func_col = self._find_col(df, ['职能'])
        scale_col = self._find_col(df, ['公司规模'])
        prod_col = self._find_col(df, ['TA生产率'])

        if not prod_col:
            self._log_error('ta_productivity', '缺少TA生产率列')
            return

        company_rows = df[df[func_col] == '公司整体'].copy() if func_col else df.copy()

        if scale_col:
            by_scale = company_rows.groupby(scale_col)[prod_col].median().reset_index()
            by_scale.columns = ['公司规模', 'TA生产率_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='productivity.by_scale',
                module='TA生产率',
                dimension='不同规模公司TA生产率',
                group_by=['公司规模'],
                metric_name='TA生产率_P50',
                metric_value=by_scale,
                formula='人效招聘总量/人力投入总量 (P50)',
                data_source='4.2_职能',
                tags=['生产率', '公司规模'],
            ))
            print("  [OK] TA生产率")

    # ==================== 细分职能报告 ====================

    def _compile_commercial_breakdown(self, dataset):
        """编译: 商业职能细分数据"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        sub_func_col = self._find_col(df, ['三级职能', '二级职能'])
        total_col = self._find_col(df, ['招聘总量'])
        tth_col = self._find_col(df, ['招聘周期'])
        cost_col = self._find_col(df, ['外部渠道费用成本', '外部渠道成本'])

        if not sub_func_col:
            self._log_error('commercial_breakdown', '缺少三级职能列')
            return

        # 招聘量占比
        if total_col:
            agg = df.groupby(sub_func_col)[total_col].sum().reset_index()
            total = agg[total_col].sum()
            agg['招聘量占比'] = agg[total_col] / total if total > 0 else np.nan
            agg.columns = ['二级职能', '招聘总量', '招聘量占比']

            self.kb.add_entry(KnowledgeEntry(
                entry_id='commercial.vol.sub_func',
                module='细分职能报告-商业',
                dimension='商业-各二级职能招聘量占比',
                group_by=['商业二级职能'],
                metric_name='招聘量占比',
                metric_value=agg,
                formula='各商业职能招聘数/FTE招聘总数',
                data_source='4.4_商业',
                tags=['商业', '二级数据', '招聘量'],
            ))

        # 招聘周期
        if tth_col:
            tth_agg = df.groupby(sub_func_col)[tth_col].median().reset_index()
            tth_agg.columns = ['二级职能', '招聘周期_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='commercial.tth.sub_func',
                module='细分职能报告-商业',
                dimension='商业-各二级职能招聘周期',
                group_by=['商业二级职能'],
                metric_name='招聘周期_P50',
                metric_value=tth_agg,
                formula='直接数据 (P50)',
                data_source='4.4_商业',
                tags=['商业', '二级数据', '招聘周期'],
            ))

        # 渠道分布
        channel_cols = {
            '猎头': self._find_col(df, ['猎头 （单位：人）', '猎头']),
            'RPO': self._find_col(df, ['RPO （单位：人）']),
            '内部推荐': self._find_col(df, ['内部推荐']),
        }
        if total_col:
            ch_results = []
            for ch_name, ch_col in channel_cols.items():
                if ch_col:
                    ch_agg = df.groupby(sub_func_col).apply(
                        lambda g: g[ch_col].sum() / g[total_col].sum()
                        if g[total_col].sum() > 0 else np.nan
                    ).reset_index()
                    ch_agg.columns = ['二级职能', '占比']
                    ch_agg['渠道'] = ch_name
                    ch_results.append(ch_agg)

            if ch_results:
                all_ch = pd.concat(ch_results, ignore_index=True)
                self.kb.add_entry(KnowledgeEntry(
                    entry_id='commercial.channel.sub_func',
                    module='细分职能报告-商业',
                    dimension='商业-各二级职能渠道招聘量占比',
                    group_by=['商业二级职能', '招聘渠道'],
                    metric_name='渠道占比',
                    metric_value=all_ch,
                    formula='各商业职能不同渠道招聘数/各商业职能FTE招聘总数',
                    data_source='4.4_商业',
                    tags=['商业', '二级数据', '渠道'],
                ))

        print("  [OK] 细分职能报告 - 商业")

    def _compile_rd_breakdown(self, dataset):
        """编译: 研发职能细分数据"""
        df = self._get_main_df(dataset)
        if df is None:
            return

        sub_func_col = self._find_col(df, ['三级职能', '二级职能'])
        total_col = self._find_col(df, ['招聘总量'])
        tth_col = self._find_col(df, ['招聘周期'])

        if not sub_func_col:
            self._log_error('rd_breakdown', '缺少三级职能列')
            return

        # 招聘量占比
        if total_col:
            agg = df.groupby(sub_func_col)[total_col].sum().reset_index()
            total = agg[total_col].sum()
            agg['招聘量占比'] = agg[total_col] / total if total > 0 else np.nan
            agg.columns = ['二级职能', '招聘总量', '招聘量占比']

            self.kb.add_entry(KnowledgeEntry(
                entry_id='rd.vol.sub_func',
                module='细分职能报告-研发',
                dimension='研发-各二级职能招聘量占比',
                group_by=['研发二级职能'],
                metric_name='招聘量占比',
                metric_value=agg,
                formula='各研发职能招聘数/FTE招聘总数',
                data_source='4.3_研发',
                tags=['研发', '二级数据', '招聘量'],
            ))

        # 招聘周期
        if tth_col:
            tth_agg = df.groupby(sub_func_col)[tth_col].median().reset_index()
            tth_agg.columns = ['二级职能', '招聘周期_P50']
            self.kb.add_entry(KnowledgeEntry(
                entry_id='rd.tth.sub_func',
                module='细分职能报告-研发',
                dimension='研发-各二级职能招聘周期',
                group_by=['研发二级职能'],
                metric_name='招聘周期_P50',
                metric_value=tth_agg,
                formula='直接数据 (P50)',
                data_source='4.3_研发',
                tags=['研发', '二级数据', '招聘周期'],
            ))

        # 渠道分布
        channel_cols = {
            '猎头': self._find_col(df, ['猎头 （单位：人）', '猎头']),
            'RPO': self._find_col(df, ['RPO （单位：人）']),
            '内部推荐': self._find_col(df, ['内部推荐']),
        }
        if total_col:
            ch_results = []
            for ch_name, ch_col in channel_cols.items():
                if ch_col:
                    ch_agg = df.groupby(sub_func_col).apply(
                        lambda g: g[ch_col].sum() / g[total_col].sum()
                        if g[total_col].sum() > 0 else np.nan
                    ).reset_index()
                    ch_agg.columns = ['二级职能', '占比']
                    ch_agg['渠道'] = ch_name
                    ch_results.append(ch_agg)

            if ch_results:
                all_ch = pd.concat(ch_results, ignore_index=True)
                self.kb.add_entry(KnowledgeEntry(
                    entry_id='rd.channel.sub_func',
                    module='细分职能报告-研发',
                    dimension='研发-各二级职能渠道招聘量占比',
                    group_by=['研发二级职能', '招聘渠道'],
                    metric_name='渠道占比',
                    metric_value=all_ch,
                    formula='各研发职能不同渠道招聘数/各研发职能FTE招聘总数',
                    data_source='4.3_研发',
                    tags=['研发', '二级数据', '渠道'],
                ))

        print("  [OK] 细分职能报告 - 研发")

    # ==================== 工具方法 ====================

    def _get_main_df(self, dataset) -> Optional[pd.DataFrame]:
        """从RawDataset中获取主数据表"""
        if hasattr(dataset, 'sheets'):
            sheets = dataset.sheets
            if len(sheets) == 1:
                return list(sheets.values())[0]
            # 优先选择第一个sheet
            return list(sheets.values())[0] if sheets else None
        return None

    def _find_col(self, df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
        """在DataFrame中查找包含关键词的列名"""
        for kw in keywords:
            for col in df.columns:
                if kw.lower() in col.lower():
                    return col
        return None

    def _log_error(self, context: str, message: str):
        """记录编译错误"""
        self._compile_errors.append({
            'context': context,
            'message': message,
        })
        print(f"  [WARN] {context}: {message}")

    def get_compile_errors(self) -> List[Dict]:
        return self._compile_errors
