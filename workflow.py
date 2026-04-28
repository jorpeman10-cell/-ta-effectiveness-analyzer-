"""
Workflow Orchestrator - 工作流编排器
完整的自动化分析工作流:

  1. INGEST  - 上传/摄入原始调研问卷
  2. COMPILE - AI处理编译为知识库
  3. PIVOT   - 调用Skill执行指标透视
  4. REPORT  - 生成分析报告
  5. VALIDATE - 自查验证准确性

使用方式:
  python workflow.py                    # 使用默认数据目录
  python workflow.py --data-dir <path>  # 指定数据目录
  python workflow.py --output <path>    # 指定输出目录
"""
import os
import sys
import json
import argparse
import datetime
from pathlib import Path

# 确保模块可导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raw_data.ingestor import FileIngestor
from raw_data.column_mapper import ColumnMapper
from wiki.compiler import DataCompiler
from wiki.knowledge_base import KnowledgeBase
from scheme.analysis_dimensions import DIMENSION_REGISTRY
from scheme.indicator_pivot import IndicatorPivot
from scheme.report_writer import ReportWriter
from scheme.validator import Validator


class WorkflowOrchestrator:
    """
    工作流编排器 - 端到端自动化分析
    
    架构:
      Raw Data (原始问卷) 
        → Wiki (AI编译知识库) 
          → Scheme (Skills执行: 分析/透视/报告/验证)
    """

    def __init__(self, data_dir: str = None, output_dir: str = None):
        self.data_dir = data_dir or r'D:\win设备桌面\2025年业绩核算\TA效能报告'
        self.output_dir = output_dir or os.path.join(self.data_dir, 'wiki_output')
        
        # 各层组件
        self.ingestor = FileIngestor(self.data_dir)
        self.mapper = ColumnMapper()
        self.compiler = DataCompiler()
        self.kb: KnowledgeBase = None
        self.pivot: IndicatorPivot = None
        self.writer: ReportWriter = None
        self.validator: Validator = None
        
        # 工作流状态
        self.raw_datasets = {}
        self.workflow_log = []

    def run(self, skip_validation: bool = False) -> dict:
        """
        执行完整工作流
        返回: 工作流执行结果摘要
        """
        start_time = datetime.datetime.now()
        
        print("\n" + "=" * 70)
        print("  TA效能报告 - LLM Wiki 自动化分析工作流")
        print("  " + "=" * 66)
        print(f"  数据目录: {self.data_dir}")
        print(f"  输出目录: {self.output_dir}")
        print(f"  启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)

        # Step 1: INGEST - 摄入原始数据
        self._step_ingest()

        # Step 2: COMPILE - 编译为知识库
        self._step_compile()

        # Step 3: PIVOT - 指标透视分析
        self._step_pivot()

        # Step 4: REPORT - 生成报告
        self._step_report()

        # Step 5: VALIDATE - 验证自查
        validation_report = None
        if not skip_validation:
            validation_report = self._step_validate()

        # 汇总
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            'status': 'SUCCESS',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'data_dir': self.data_dir,
            'output_dir': self.output_dir,
            'raw_datasets_count': len(self.raw_datasets),
            'kb_entries_count': len(self.kb.entries) if self.kb else 0,
            'pivot_results_count': len(self.pivot.results) if self.pivot else 0,
            'validation_status': validation_report.overall_status if validation_report else 'SKIPPED',
            'output_files': self._list_output_files(),
        }

        # 保存工作流日志
        self._save_workflow_log(summary)

        print("\n" + "=" * 70)
        print("  工作流执行完成!")
        print(f"  耗时: {duration:.1f}秒")
        print(f"  知识库条目: {summary['kb_entries_count']}")
        print(f"  透视结果: {summary['pivot_results_count']}")
        print(f"  验证状态: {summary['validation_status']}")
        print(f"  输出目录: {self.output_dir}")
        print("=" * 70)

        return summary

    # ==================== 工作流步骤 ====================

    def _step_ingest(self):
        """Step 1: 摄入原始数据"""
        self._log_step("STEP 1: INGEST - 摄入原始调研问卷")
        
        self.raw_datasets = self.ingestor.ingest_questionnaire_set()
        
        if not self.raw_datasets:
            print("  [WARN] 未找到任何数据文件，尝试从主目录摄入...")
            datasets = self.ingestor.ingest_directory(self.data_dir)
            for ds in datasets:
                self.raw_datasets[ds.source_name] = ds

        # 保存摄入摘要
        ingest_summary = {}
        for key, ds in self.raw_datasets.items():
            ingest_summary[key] = ds.to_summary()
        
        self._save_json('01_ingest_summary.json', ingest_summary)
        
        # 生成列名映射报告
        for key, ds in self.raw_datasets.items():
            for sheet_name, df in ds.sheets.items():
                report = self.mapper.generate_mapping_report(df.columns.tolist())
                safe_key = key.replace('/', '_').replace('\\', '_')
                self._save_text(f'01_column_mapping_{safe_key}_{sheet_name}.md', report)

        print(f"  [OK] 已摄入 {len(self.raw_datasets)} 个数据集")

    def _step_compile(self):
        """Step 2: 编译为知识库"""
        self._log_step("STEP 2: COMPILE - 编译为Wiki知识库")
        
        self.kb = self.compiler.compile_all(self.raw_datasets)
        
        # 保存知识库
        self.kb.export_to_json(os.path.join(self.output_dir, '02_knowledge_base.json'))
        
        # 保存知识库摘要
        kb_summary = self.kb.export_summary_markdown()
        self._save_text('02_knowledge_base_summary.md', kb_summary)
        
        # 保存维度覆盖率报告
        kb_ids = list(self.kb.entries.keys())
        coverage = DIMENSION_REGISTRY.get_coverage_report(kb_ids)
        self._save_text('02_dimension_coverage.md', coverage)
        
        # 保存编译错误
        errors = self.compiler.get_compile_errors()
        if errors:
            self._save_json('02_compile_errors.json', errors)

        print(f"  [OK] 知识库已编译: {len(self.kb.entries)} 条知识条目")

    def _step_pivot(self):
        """Step 3: 指标透视分析"""
        self._log_step("STEP 3: PIVOT - 指标透视与交叉分析")
        
        self.pivot = IndicatorPivot(self.kb)
        self.pivot.pivot_all()
        
        # 保存透视摘要
        pivot_summary = self.pivot.export_pivot_summary()
        self._save_text('03_pivot_summary.md', pivot_summary)
        
        # 保存所有关键发现
        highlights = self.pivot.get_all_highlights()
        self._save_json('03_highlights.json', highlights)

        print(f"  [OK] 透视分析完成: {len(self.pivot.results)} 个维度")

    def _step_report(self):
        """Step 4: 生成报告"""
        self._log_step("STEP 4: REPORT - 生成分析报告")
        
        self.writer = ReportWriter(self.kb, self.pivot)
        
        # 生成Markdown报告
        md_report = self.writer.generate_full_report()
        self._save_text('04_analysis_report.md', md_report)
        
        # 生成结构化JSON数据
        structured = self.writer.export_structured_data()
        self._save_json('04_structured_report.json', structured)

        print(f"  [OK] 报告已生成")

    def _step_validate(self):
        """Step 5: 验证自查"""
        self._log_step("STEP 5: VALIDATE - 数据验证与自查")
        
        self.validator = Validator(self.kb)
        report = self.validator.validate_all()
        
        # 保存验证报告
        validation_md = report.to_markdown()
        self._save_text('05_validation_report.md', validation_md)

        return report

    # ==================== 工具方法 ====================

    def _log_step(self, message: str):
        """记录工作流步骤"""
        timestamp = datetime.datetime.now().isoformat()
        self.workflow_log.append({'timestamp': timestamp, 'message': message})
        print(f"\n{'─' * 60}")
        print(f"  {message}")
        print(f"{'─' * 60}")

    def _save_json(self, filename: str, data: any):
        """保存JSON文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def _save_text(self, filename: str, content: str):
        """保存文本文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

    def _save_workflow_log(self, summary: dict):
        """保存工作流日志"""
        log_data = {
            'summary': summary,
            'steps': self.workflow_log,
        }
        self._save_json('00_workflow_log.json', log_data)

    def _list_output_files(self) -> list:
        """列出输出目录中的文件"""
        if os.path.exists(self.output_dir):
            return sorted(os.listdir(self.output_dir))
        return []

    # ==================== 单步执行接口 ====================

    def ingest_single_file(self, filepath: str, key: str = None) -> dict:
        """单独摄入一个文件"""
        ds = self.ingestor.ingest_file(filepath)
        k = key or ds.source_name
        self.raw_datasets[k] = ds
        return ds.to_summary()

    def get_knowledge_base(self) -> KnowledgeBase:
        """获取知识库实例"""
        return self.kb

    def query_indicator(self, entry_id: str) -> dict:
        """查询单个指标"""
        if self.kb is None:
            return {'error': '知识库未初始化，请先运行compile步骤'}
        
        entry = self.kb.get_entry(entry_id)
        if entry is None:
            return {'error': f'未找到指标: {entry_id}'}
        
        return entry.to_dict()


def main():
    parser = argparse.ArgumentParser(description='TA效能报告 - LLM Wiki 自动化分析工作流')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='数据目录路径')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='输出目录路径')
    parser.add_argument('--skip-validation', action='store_true',
                        help='跳过验证步骤')
    args = parser.parse_args()

    orchestrator = WorkflowOrchestrator(
        data_dir=args.data_dir,
        output_dir=args.output,
    )
    
    summary = orchestrator.run(skip_validation=args.skip_validation)
    
    # 返回退出码
    if summary.get('validation_status') == 'FAIL':
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
