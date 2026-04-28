"""
Raw Data Ingestor - 原始调研问卷摄入模块
支持 Excel (.xlsx/.xls), CSV, PDF 等格式的原始问卷文件
将原始文件解析为统一的 RawDataset 结构
"""
import os
import json
import hashlib
import datetime
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any


# ============================================================
# 公司规模真值表 - 覆盖汇总表中的错误分类
# A类: ≥1500人; B类: <1500人
# ============================================================
KNOWN_COMPANY_SCALE = {
    # A类公司
    '辉致': 'A', 'Viatris': 'A', '晖致': 'A',
    '辉瑞': 'A', 'Pfizer': 'A',
    '科赴': 'A', 'Kenvue': 'A',
    '诺华': 'A', 'Novartis': 'A',
    '卫材': 'A', 'Eisai': 'A',
    '信达': 'A', 'Innovent': 'A',
    '罗氏': 'A', 'Roche': 'A',
    '百济神州': 'A', 'BeiGene': 'A', 'BeOne': 'A',
    '默沙东': 'A', 'MSD': 'A',
    '赛诺菲': 'A', 'Sanofi': 'A', 'SA': 'A',
    '默克雪兰诺': 'A', 'Merck': 'A', '默克': 'A',
    'BMS': 'A', '百时美施贵宝': 'A',
    '吉利德': 'A', 'Gilead': 'A', 'GE': 'A',  # GE=6900人
    # B类公司
    '迪哲': 'B', 'Dizal': 'B',
    '雅培': 'B', 'Abbott': 'B',
    '参天': 'B', 'Santen': 'B',
    '欧加隆': 'B', 'Organon': 'B',
    '艾伯维': 'B', 'AbbVie': 'B', 'ABV': 'B',
    'SMPC': 'B', 'MPCN': 'B',
    '丸红': 'B',
    '逸华': 'B', '逸华制药': 'B',
}


def fix_company_scale(df: pd.DataFrame) -> pd.DataFrame:
    """
    修正汇总表中公司规模分类的错误值。
    使用 KNOWN_COMPANY_SCALE 真值表覆盖 '公司规模' 列。
    
    Args:
        df: 包含 '所属公司' 和 '公司规模' 列的DataFrame
    Returns:
        修正后的DataFrame
    """
    company_col = None
    scale_col = None
    
    for c in df.columns:
        if '所属公司' in str(c) or '公司名称' in str(c):
            company_col = c
        if '公司规模' in str(c) and '分类' not in str(c):
            scale_col = c
    
    if company_col is None or scale_col is None:
        return df  # 没有相关列，不处理
    
    fix_count = 0
    for idx, row in df.iterrows():
        company_name = str(row[company_col]).strip()
        old_scale = str(row[scale_col]).strip()
        
        # 在真值表中查找匹配
        new_scale = None
        for key, cls in KNOWN_COMPANY_SCALE.items():
            if key in company_name or company_name in key:
                new_scale = cls
                break
        
        if new_scale and new_scale != old_scale:
            df.at[idx, scale_col] = new_scale
            fix_count += 1
    
    if fix_count > 0:
        print(f"  [FIX] 修正了 {fix_count} 行公司规模分类")
    
    return df


class RawDataset:
    """原始数据集的统一表示"""

    def __init__(self, source_path: str):
        self.source_path = source_path
        self.source_name = os.path.basename(source_path)
        self.file_hash = self._compute_hash(source_path)
        self.ingested_at = datetime.datetime.now().isoformat()
        self.sheets: Dict[str, pd.DataFrame] = {}
        self.metadata: Dict[str, Any] = {}
        self.column_registry: Dict[str, List[str]] = {}
        self.data_quality: Dict[str, Any] = {}

    @staticmethod
    def _compute_hash(filepath: str) -> str:
        """计算文件MD5哈希，用于变更检测"""
        h = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()

    def add_sheet(self, name: str, df: pd.DataFrame):
        """添加一个sheet的数据"""
        self.sheets[name] = df
        self.column_registry[name] = df.columns.tolist()

    def get_sheet(self, name: str) -> Optional[pd.DataFrame]:
        return self.sheets.get(name)

    def list_sheets(self) -> List[str]:
        return list(self.sheets.keys())

    def to_summary(self) -> Dict:
        """生成数据集摘要"""
        return {
            'source': self.source_name,
            'hash': self.file_hash,
            'ingested_at': self.ingested_at,
            'sheets': {
                name: {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns.tolist(),
                    'dtypes': {col: str(dt) for col, dt in df.dtypes.items()},
                    'null_counts': df.isnull().sum().to_dict(),
                }
                for name, df in self.sheets.items()
            },
            'data_quality': self.data_quality,
        }


class FileIngestor:
    """文件摄入器 - 支持多种格式"""

    # 标准化替换规则
    REPLACE_VALUES = {'不填写': np.nan, '': np.nan, 'NaN': np.nan, 'None': np.nan, '-': np.nan}

    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or r'D:\win设备桌面\2025年业绩核算\TA效能报告'
        self.source_dir = os.path.join(self.data_dir, '数据源')

    def ingest_file(self, filepath: str, header_row: int = 0) -> RawDataset:
        """摄入单个文件"""
        ext = Path(filepath).suffix.lower()
        dataset = RawDataset(filepath)

        if ext in ('.xlsx', '.xls'):
            self._ingest_excel(dataset, filepath, header_row)
        elif ext == '.csv':
            self._ingest_csv(dataset, filepath)
        else:
            raise ValueError(f"不支持的文件格式: {ext}")

        # 数据质量评估
        dataset.data_quality = self._assess_quality(dataset)
        return dataset

    def ingest_directory(self, directory: str = None, pattern: str = '*.xlsx') -> List[RawDataset]:
        """批量摄入目录下的所有匹配文件"""
        target_dir = directory or self.source_dir
        datasets = []
        for f in Path(target_dir).glob(pattern):
            if f.name.startswith('~$'):
                continue
            try:
                ds = self.ingest_file(str(f))
                datasets.append(ds)
            except Exception as e:
                print(f"[WARN] 跳过文件 {f.name}: {e}")
        return datasets

    def ingest_questionnaire_set(self) -> Dict[str, RawDataset]:
        """摄入标准调研问卷集 (4.1~4.7)"""
        standard_files = {
            '4.1_职级': '4.1 TA公司整体效率汇总表-职级.xlsx',
            '4.2_职能': '4.2 TA公司整体效率汇总表-职能.xlsx',
            '4.3_研发': '4.3 部门效率汇总表-研发.xlsx',
            '4.4_商业': '4.4 部门效率汇总表-商业.xlsx',
            '4.5_职能TA': '4.5 职能TA效率汇总表.xlsx',
            '4.7_问卷': '4.7 综合招聘效能提升汇总表.xlsx',
        }

        results = {}
        for key, filename in standard_files.items():
            filepath = self._find_file(filename)
            if filepath:
                try:
                    results[key] = self.ingest_file(filepath, header_row=1)
                    print(f"[OK] 已摄入: {key} ({filename})")
                except Exception as e:
                    print(f"[ERR] 摄入失败 {key}: {e}")
            else:
                print(f"[MISS] 未找到: {filename}")

        return results

    def _find_file(self, filename: str) -> Optional[str]:
        """在数据源目录和主目录中查找文件"""
        for d in [self.source_dir, self.data_dir]:
            p = os.path.join(d, filename)
            if os.path.exists(p):
                return p
        return None

    def _ingest_excel(self, dataset: RawDataset, filepath: str, header_row: int):
        """摄入Excel文件的所有sheet"""
        xl = pd.ExcelFile(filepath)
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(filepath, sheet_name=sheet_name, header=header_row)
                df = self._clean_dataframe(df)
                dataset.add_sheet(sheet_name, df)
            except Exception as e:
                print(f"  [WARN] Sheet '{sheet_name}' 读取失败: {e}")

        dataset.metadata['excel_sheet_count'] = len(xl.sheet_names)
        dataset.metadata['excel_sheet_names'] = xl.sheet_names

    def _ingest_csv(self, dataset: RawDataset, filepath: str):
        """摄入CSV文件"""
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        df = self._clean_dataframe(df)
        dataset.add_sheet('main', df)

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """统一数据清洗"""
        # 标准化列名
        df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]

        # 去除完全空行
        df = df.dropna(how='all').reset_index(drop=True)

        # 替换标准无效值
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].replace(self.REPLACE_VALUES)

        # 自动数值化尝试
        for col in df.columns:
            if df[col].dtype == object:
                converted = pd.to_numeric(df[col], errors='coerce')
                valid_ratio = converted.notna().sum() / max(df[col].notna().sum(), 1)
                if valid_ratio > 0.8:
                    df[col] = converted

        # 修正公司规模分类（使用真值表覆盖汇总表中的错误值）
        df = fix_company_scale(df)

        return df

    def _assess_quality(self, dataset: RawDataset) -> Dict:
        """评估数据质量"""
        quality = {
            'total_sheets': len(dataset.sheets),
            'total_rows': sum(len(df) for df in dataset.sheets.values()),
            'total_columns': sum(len(df.columns) for df in dataset.sheets.values()),
            'completeness': {},
            'issues': [],
        }

        for name, df in dataset.sheets.items():
            total_cells = df.shape[0] * df.shape[1]
            null_cells = df.isnull().sum().sum()
            completeness = 1 - (null_cells / total_cells) if total_cells > 0 else 0
            quality['completeness'][name] = round(completeness, 4)

            # 检查高缺失率列
            for col in df.columns:
                null_rate = df[col].isnull().sum() / len(df)
                if null_rate > 0.5:
                    quality['issues'].append({
                        'sheet': name,
                        'column': col,
                        'issue': f'高缺失率 ({null_rate:.1%})',
                    })

        return quality


if __name__ == '__main__':
    ingestor = FileIngestor()
    datasets = ingestor.ingest_questionnaire_set()
    for key, ds in datasets.items():
        summary = ds.to_summary()
        print(f"\n=== {key} ===")
        for sheet, info in summary['sheets'].items():
            print(f"  Sheet '{sheet}': {info['rows']} rows x {info['columns']} cols")
