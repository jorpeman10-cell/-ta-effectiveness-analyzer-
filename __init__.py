"""
TA效能报告 - LLM Wiki 自动化分析系统
架构: Raw Data → Wiki (知识库) → Scheme (Skills执行)

三层架构:
  1. raw_data/   - 原始数据摄入与解析
  2. wiki/       - AI处理后的结构化知识库
  3. scheme/     - Skills执行层 (分析维度、指标透视、报告撰写)
  4. workflow.py - 工作流编排器
"""

__version__ = "1.0.0"
