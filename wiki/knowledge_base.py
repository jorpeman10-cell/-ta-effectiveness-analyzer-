"""
Knowledge Base - 知识库核心模块
将原始数据编译为结构化的知识条目(KnowledgeEntry)
支持按维度、指标、公司规模等多维检索
"""
import json
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict


@dataclass
class KnowledgeEntry:
    """知识条目 - Wiki中的最小知识单元"""
    entry_id: str                    # 唯一标识 e.g. "recruitment_volume.function.commercial"
    module: str                      # 所属模块 e.g. "招聘量指标"
    dimension: str                   # 分析维度 e.g. "各职能招聘量占比"
    group_by: List[str]              # 分组维度 e.g. ["职能", "公司规模"]
    metric_name: str                 # 指标名称 e.g. "招聘总量占比_P50"
    metric_value: Any                # 指标值 (可以是数值、DataFrame、字典)
    formula: str                     # 计算公式描述
    data_source: str                 # 数据来源 e.g. "4.2_职能"
    computed_at: str = ""            # 计算时间
    confidence: float = 1.0          # 置信度 (0-1)
    tags: List[str] = field(default_factory=list)
    notes: str = ""                  # 备注

    def __post_init__(self):
        if not self.computed_at:
            self.computed_at = datetime.datetime.now().isoformat()

    def to_dict(self) -> Dict:
        """转为可序列化的字典"""
        d = asdict(self)
        # DataFrame需要特殊处理
        if isinstance(d['metric_value'], pd.DataFrame):
            d['metric_value'] = d['metric_value'].to_dict('records')
        elif isinstance(d['metric_value'], (np.integer, np.floating)):
            d['metric_value'] = float(d['metric_value'])
        elif isinstance(d['metric_value'], np.ndarray):
            d['metric_value'] = d['metric_value'].tolist()
        return d


class KnowledgeBase:
    """知识库 - 管理所有知识条目"""

    def __init__(self):
        self.entries: Dict[str, KnowledgeEntry] = {}
        self.compile_log: List[Dict] = []
        self.created_at = datetime.datetime.now().isoformat()

    def add_entry(self, entry: KnowledgeEntry):
        """添加知识条目"""
        self.entries[entry.entry_id] = entry
        self.compile_log.append({
            'action': 'add',
            'entry_id': entry.entry_id,
            'timestamp': datetime.datetime.now().isoformat(),
        })

    def get_entry(self, entry_id: str) -> Optional[KnowledgeEntry]:
        return self.entries.get(entry_id)

    def query_by_module(self, module: str) -> List[KnowledgeEntry]:
        """按模块查询"""
        return [e for e in self.entries.values() if e.module == module]

    def query_by_dimension(self, dimension: str) -> List[KnowledgeEntry]:
        """按分析维度查询"""
        return [e for e in self.entries.values() if dimension in e.dimension]

    def query_by_tags(self, tags: List[str]) -> List[KnowledgeEntry]:
        """按标签查询"""
        return [e for e in self.entries.values()
                if any(t in e.tags for t in tags)]

    def query_by_group(self, group_key: str) -> List[KnowledgeEntry]:
        """按分组维度查询"""
        return [e for e in self.entries.values()
                if group_key in e.group_by]

    def list_modules(self) -> List[str]:
        """列出所有模块"""
        return list(set(e.module for e in self.entries.values()))

    def list_dimensions(self) -> List[str]:
        """列出所有分析维度"""
        return list(set(e.dimension for e in self.entries.values()))

    def get_statistics(self) -> Dict:
        """获取知识库统计信息"""
        return {
            'total_entries': len(self.entries),
            'modules': self.list_modules(),
            'dimensions': self.list_dimensions(),
            'created_at': self.created_at,
            'last_updated': max(
                (e.computed_at for e in self.entries.values()),
                default=self.created_at
            ),
        }

    def export_to_json(self, filepath: str):
        """导出知识库为JSON"""
        data = {
            'metadata': self.get_statistics(),
            'entries': {k: v.to_dict() for k, v in self.entries.items()},
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def export_summary_markdown(self) -> str:
        """导出知识库摘要为Markdown"""
        lines = [
            "# TA效能分析知识库摘要",
            f"\n生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"\n总条目数: {len(self.entries)}",
            "",
        ]

        # 按模块分组展示
        for module in sorted(self.list_modules()):
            entries = self.query_by_module(module)
            lines.append(f"\n## {module} ({len(entries)}条)")
            lines.append("")
            lines.append("| 维度 | 分组 | 指标 | 数据源 | 置信度 |")
            lines.append("|------|------|------|--------|--------|")
            for e in entries:
                group_str = " × ".join(e.group_by)
                conf_str = f"{e.confidence:.0%}"
                lines.append(
                    f"| {e.dimension} | {group_str} | {e.metric_name} | {e.data_source} | {conf_str} |"
                )

        return "\n".join(lines)
