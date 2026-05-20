# -*- coding: utf-8 -*-
"""
Survey Parser - 问卷调研 Sheets 3/4/5 解析器
解析问卷中的定性调研数据，生成行业趋势分析

Sheet 3: 综合招聘效能提升 (28道选择题, 涵盖7大模块)
Sheet 4: 热点职位调研 (2026年新增HC预测 + 热点职能)
Sheet 5: 高管任期变化趋势调研 (高管任期缩短趋势)
"""
import os
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from typing import Dict, List, Optional, Tuple


# ============================================================
# Sheet 3: 综合招聘效能提升
# ============================================================

# 问题定义: {题号: (模块, 问题文本, 选项类型, 选项列表)}
SHEET3_QUESTIONS = {
    '1.1': ('Employer Branding', '我们内部是否实施了雇主品牌建设？', 'single', [
        'A.是，我们有明确的雇主品牌战略并在实施中',
        'B.部分实施，但没有完整的战略',
        'C.否，目前没有进行雇主品牌建设',
        'D.不清楚，没有关注过此类项目',
    ]),
    '1.2': ('Employer Branding', '我们如何通过雇主品牌吸引优秀人才？', 'single', [
        'A.提供具有竞争力的薪酬和福利',
        'B.利用社交媒体提升雇主品牌的可见度',
        'C.打造吸引人才的职业发展机会',
        'D.有激励措施的员工内推计划',
    ]),
    '1.3': ('Employer Branding', '我们如何通过员工来传播雇主品牌？', 'single', [
        'A.鼓励员工在社交媒体上分享公司文化和活动',
        'B.通过员工推荐计划激励内部员工参与招聘',
        'C.提供培训，使员工更好地传播品牌价值',
        'D.目前没有通过员工传播品牌的措施',
    ]),
    '1.4': ('Employer Branding', '通过雇主品牌的实施，给企业带来了怎样的影响？', 'single', [
        'A.降低offer拒绝率',
        'B.提升内推项目的成功转正率',
        'C.提升品牌关注度和职位搜索量',
        'D.提升校园招聘影响力',
    ]),
    '2.1': ('Talent Acquisition Strategy', '我们如何确保候选人的价值观与企业文化相匹配？', 'single', [
        'A.通过价值观相关的面试问题进行评估',
        'B.通过情景模拟或案例分析了解候选人的反应',
        'C.通过背景调查或推荐人意见进行验证',
        'D.目前没有专门针对价值观匹配的评估方式',
    ]),
    '2.2': ('Talent Acquisition Strategy', '我们如何衡量候选人的核心能力以及长期发展潜力？', 'single', [
        'A.通过技能测试和专业面试评估核心能力',
        'B.通过发展潜力相关的面试问题进行判断',
        'C.通过试用期表现验证候选人能力和潜力',
        'D.目前没有系统的方法衡量候选人的能力和潜力',
    ]),
    '2.3': ('Talent Acquisition Strategy', '针对不同岗位是否制定了特定的招聘策略？', 'single', [
        'A.是的，每个岗位都有明确的招聘策略',
        'B.部分岗位有，但不是全部',
        'C.没有针对岗位的差异化策略',
        'D.不清楚，招聘策略主要依赖统一的流程',
    ]),
    '3.1': ('Recruitment Process Optimization', '我们的面试流程是否标准化？', 'single', [
        'A.完全标准化，所有职位的面试都有明确的流程和指南',
        'B.部分标准化，但不同部门和岗位的流程不统一',
        'C.流程不够标准化，主要依赖面试官的个人经验',
        'D.没有标准化流程',
    ]),
    '3.2': ('Recruitment Process Optimization', '招聘流程中，我们在哪些环节的介入能够提升招聘有效性？', 'multi', [
        'A.招聘岗位需求分析阶段',
        'B.简历筛选阶段',
        'C.面试安排与评估阶段',
        'D.Offer谈判阶段',
        'E.入职准备与培训阶段',
    ]),
    '3.3': ('Recruitment Process Optimization', '我们在招聘中的权限边界体现在哪里？', 'multi', [
        'A.招聘预算和资源的调整需要更高层级批准',
        'B.HR协助用人部门澄清岗位职责和要求',
        'C.可主导招聘流程中的具体环节',
        'D.只能建议录用，具体决策权仍归属用人部门',
        'E.超预算的Offer需报上级审批',
    ]),
    '3.4': ('Recruitment Process Optimization', '我们是否给面试官提供了足够的培训？', 'single', [
        'A.是的，定期提供面试技巧和流程管理培训',
        'B.偶尔提供培训，但频率和内容不足',
        'C.面试官主要依赖个人经验，缺乏系统的培训支持',
        'D.没有为面试官提供专门的培训',
    ]),
    '3.5': ('Recruitment Process Optimization', '我们是否利用了技术手段来提高面试效率？', 'single', [
        'A.是的，使用了视频面试、AI筛选工具或招聘管理系统',
        'B.部分岗位使用了技术手段，但未全面推广',
        'C.没有利用技术手段，完全依靠人工操作',
        'D.没有使用技术工具',
    ]),
    '4.1': ('Candidate Experience Management', '我们如何确保候选人在整个招聘流程中有良好的体验？', 'single', [
        'A.提供清晰的流程说明和快速反馈机制',
        'B.使用友好的线上招聘平台或工具',
        'C.面试官表现专业并尊重候选人',
        'D.目前没有特别的措施',
    ]),
    '4.2': ('Candidate Experience Management', '我们如何收集和处理候选人的反馈？', 'single', [
        'A.提供在线问卷收集反馈，并定期总结改进',
        'B.招聘结束后通过电话或邮件询问候选人意见',
        'C.通过第三方机构进行满意度调研',
        'D.没有正式的反馈收集和处理机制',
    ]),
    '4.3': ('Candidate Experience Management', '我们如何改进候选人的面试体验？', 'single', [
        'A.优化面试流程，减少等待时间',
        'B.提供详细的面试前准备说明',
        'C.选择合适的面试官并进行面试技巧培训',
        'D.目前没有具体的改进计划',
    ]),
    '5.1': ('Performance Management', '我们是否定期评估和调整招聘策略？', 'single', [
        'A.是的，定期评估并基于招聘周期、质量和成本调整策略',
        'B.设定了部分指标，但缺乏系统性的评估',
        'C.评估较少，主要依赖临时反馈调整策略',
        'D.没有明确的招聘关键绩效指标',
    ]),
    '5.2': ('Performance Management', '我们设定了哪些招聘关键绩效指标？', 'single', [
        'A.招聘计划完成率',
        'B.招聘质量(试用期通过率、新员工业绩达标率等)',
        'C.招聘周期(从发布职位到候选人入职的时间)',
        'D.招聘成本',
    ]),
    '5.3': ('Performance Management', '在招聘绩效评估中，公司如何识别和解决流程中的瓶颈问题？', 'single', [
        'A.通过数据分析和候选人反馈识别瓶颈',
        'B.定期召开内部会议讨论招聘问题',
        'C.临时发现问题后再进行调整',
        'D.没有特别的绩效评估机制',
    ]),
    '5.4': ('Performance Management', '我们如何评估不同招聘渠道的效果和成本效益？', 'single', [
        'A.通过数据分析(如招聘周期、选人质量和留存率)定期评估',
        'B.通过招聘团队的主观经验判断渠道效果',
        'C.仅关注招聘成本，没有系统评估渠道效益',
        'D.目前没有针对招聘渠道的评估机制',
    ]),
    '6.1': ('Talent Retention and Development', '我们如何提高候选人接受工作邀约的可能性？', 'single', [
        'A.提供具有竞争力的薪酬福利',
        'B.展示职业发展机会和培训计划',
        'C.与候选人保持良好的沟通和跟进',
        'D.目前没有针对邀约接受率的优化措施',
    ]),
    '6.2': ('Talent Retention and Development', '我们的薪酬福利在哪些方面具有竞争力？', 'single', [
        'A.提供高于市场平均水平的薪酬待遇',
        'B.丰富的绩效奖金和中长期激励计划',
        'C.提供多元化的员工福利(住房补贴、异地补贴、商业保险等)',
        'D.有灵活的休假政策(带薪假期、远程办公等)',
    ]),
    '6.3': ('Talent Retention and Development', '我们提供了哪些有效的职业发展和晋升的机会？', 'single', [
        'A.定期提供岗位相关的专业培训和学习资源',
        'B.清晰的职业晋升路径和内部竞聘机制',
        'C.跨部门轮岗或内部调动机会',
        'D.提供导师制或高潜力人才培养项目',
    ]),
    '7.1': ('Hitting Rate', '相比去年而言，今年我们的Hitting Rate整体是上升还是下降？', 'single', []),
    '7.2': ('Hitting Rate', 'Hitting Rate在哪些职能领域有所上升？', 'single', []),
    '7.3': ('Hitting Rate', 'Hitting Rate上升的原因主要是什么？', 'multi', []),
    '7.4': ('Hitting Rate', 'Hitting Rate在哪些职能领域有所下降？', 'single', []),
    '7.5': ('Hitting Rate', 'Hitting Rate下降的原因主要是什么？', 'multi', []),
    '8.1': ('Cost for per hiring', '我们的招聘成本包含的人员类型有哪些？', 'single', [
        'A.正式员工',
        'B.实习生',
        'C.第三方员工',
    ]),
    '8.2': ('Cost for per hiring', '我们的招聘成本包含费用的口径有哪些？', 'single', [
        'A.猎头费',
        'B.RPO费用',
        'C.内推费用',
        'D.TA员工费用',
    ]),
}

# Sheet 5: 高管任期问题定义
SHEET5_QUESTIONS = {
    '1.1': ('高管任期现状感知', '您是否观察到高管岗位的服务周期明显缩短？', 'single', []),
    '1.2': ('高管任期现状感知', '若有，主要集中在哪些岗位？', 'multi', []),
    '1.3': ('高管任期现状感知', '这些岗位的平均实际任期目前约为多久？', 'single', []),
    '1.4': ('高管任期现状感知', '与过去2-4年的传统任期相比，缩短幅度约为多少？', 'text', []),
    '2.1': ('任期缩短的驱动因素分析', '您认为导致高管任期缩短的核心原因有哪些？', 'multi', []),
    '2.2': ('任期缩短的驱动因素分析', '哪些外部环境变化对高管稳定性影响最大？', 'multi', []),
    '3.1': ('组织与人才影响评估', '高管频繁更替对组织产生了哪些具体影响？', 'multi', []),
    '3.2': ('组织与人才影响评估', '组织与人才影响评估补充题', 'multi', []),
    '4.1': ('企业应对策略', '面对高管任期缩短的趋势，企业在人才选聘上是否调整了标准？', 'multi', []),
    '4.2': ('企业应对策略', '在高管入职后，为了应对短期化任期，公司采取了哪些策略？', 'multi', []),
    '5.1': ('未来展望与建议', '您是否认为短期化任期已成为一种"新常态"？', 'single', []),
    '5.2': ('未来展望与建议', '您认为未来1-2年内，高管岗位的服务周期会继续缩短吗？', 'single', []),
    '5.3': ('未来展望与建议', '您认为理想的高管任期应为多久？', 'single', []),
    '5.4': ('未来展望与建议', '如何平衡短期业绩与长期发展？', 'text', []),
    '5.5': ('未来展望与建议', '您希望组织人才专委会在应对这一趋势方面提供哪些支持？', 'multi', []),
}


def _find_sheet(sheet_names, keyword):
    """根据关键词找到sheet名"""
    for name in sheet_names:
        if keyword in str(name):
            return name
    return None


def _archive_path(base: str, target: str) -> str:
    return posixpath.normpath(posixpath.join(posixpath.dirname(base), target)).lstrip('/')


def _get_sheet_xml_path(zf: zipfile.ZipFile, sheet_name: str) -> Optional[str]:
    ns = {
        'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
        'rel': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        'pkg': 'http://schemas.openxmlformats.org/package/2006/relationships',
    }
    wb = ET.fromstring(zf.read('xl/workbook.xml'))
    rels = ET.fromstring(zf.read('xl/_rels/workbook.xml.rels'))
    rid_to_target = {
        rel.attrib['Id']: rel.attrib['Target']
        for rel in rels.findall('pkg:Relationship', ns)
    }
    for sheet in wb.findall('main:sheets/main:sheet', ns):
        if sheet.attrib.get('name') == sheet_name:
            rid = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
            target = rid_to_target.get(rid)
            return _archive_path('xl/workbook.xml', target) if target else None
    return None


def _get_vml_path_for_sheet(zf: zipfile.ZipFile, sheet_xml_path: str) -> Optional[str]:
    rels_path = posixpath.join(
        posixpath.dirname(sheet_xml_path),
        '_rels',
        posixpath.basename(sheet_xml_path) + '.rels',
    )
    if rels_path not in zf.namelist():
        return None
    ns = {'pkg': 'http://schemas.openxmlformats.org/package/2006/relationships'}
    rels = ET.fromstring(zf.read(rels_path))
    for rel in rels.findall('pkg:Relationship', ns):
        rel_type = rel.attrib.get('Type', '')
        if rel_type.endswith('/vmlDrawing'):
            return _archive_path(sheet_xml_path, rel.attrib.get('Target', ''))
    return None


def _clean_vml_label(value: str) -> str:
    """Normalize VML form-control label text."""
    if not value:
        return ''
    value = re.sub(r'<[^>]+>', '', value)
    value = value.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    value = value.replace('&quot;', '"').replace('&apos;', "'")
    return re.sub(r'\s+', ' ', value).strip()


def _extract_vml_checked_options(filepath: str, sheet_name: str, prefer_labels: bool = False) -> Dict[int, List[str]]:
    """
    Extract checked Excel form controls from a sheet's VML drawing.

    Returns a mapping of zero-based row index -> selected options.
    Excel stores many survey answers as form controls, not cell values.
    When prefer_labels is True, checked controls return their own labels
    (for example "B.研发负责人") instead of a position-derived letter.
    """
    try:
        with zipfile.ZipFile(filepath) as zf:
            sheet_xml = _get_sheet_xml_path(zf, sheet_name)
            if not sheet_xml:
                return {}
            vml_path = _get_vml_path_for_sheet(zf, sheet_xml)
            if not vml_path or vml_path not in zf.namelist():
                return {}
            text = zf.read(vml_path).decode('utf-8', errors='ignore')
    except Exception:
        return {}

    row_controls = defaultdict(list)
    shape_pattern = re.compile(r'<v:shape\b(.*?)</v:shape>', re.S)
    for shape_match in shape_pattern.finditer(text):
        shape_block = shape_match.group(0)
        match = re.search(r'<x:ClientData ObjectType="([^"]+)">(.*?)</x:ClientData>', shape_block, re.S)
        if not match:
            continue
        obj_type = match.group(1)
        if obj_type not in ('Radio', 'Checkbox'):
            continue
        block = match.group(2)
        anchor = re.search(r'<x:Anchor>\s*([\d,\s]+)</x:Anchor>', block, re.S)
        if not anchor:
            continue
        nums = [int(x.strip()) for x in anchor.group(1).split(',') if x.strip().isdigit()]
        if len(nums) < 4:
            continue
        col_idx, row_idx = nums[0], nums[2]
        checked = '<x:Checked>1</x:Checked>' in block
        label = ''
        label_match = re.search(r'<div[^>]*>(.*?)</div>', shape_block, re.S)
        if label_match:
            label = _clean_vml_label(label_match.group(1))
        row_controls[row_idx].append((col_idx, checked, label))

    selected = {}
    for row_idx, controls in row_controls.items():
        positions = sorted({col for col, _, _ in controls})
        col_to_letter = {col: chr(ord('A') + i) for i, col in enumerate(positions)}
        letters = []
        for col, checked, label in controls:
            if checked:
                answer = label if prefer_labels and label else col_to_letter.get(col)
                if answer and answer not in letters:
                    letters.append(answer)
        if letters:
            selected[row_idx] = letters
    return selected


def _extract_answer(row, start_col=3, max_col=15):
    """
    从行数据中提取选择的答案。
    问卷格式：选项答案通常在第4列(index 3)开始的单元格中，
    被选中的选项会显示其文本值(如 "A.是的...")，未选中的为空或NaN
    """
    answers = []
    for col_idx in range(start_col, min(max_col, len(row))):
        val = row[col_idx] if col_idx < len(row) else None
        if pd.notna(val):
            val_str = str(val).strip()
            # Skip template text like "E.其他（填写此单元格）"
            if val_str and '填写此单元格' not in val_str and val_str != '不填写':
                # Extract option letter
                if val_str and val_str[0] in 'ABCDEFG' and ('.' in val_str[:3] or '、' in val_str[:3]):
                    answers.append(val_str[0])  # Just the letter
                elif val_str:
                    answers.append(val_str)  # Free text answer
    return answers


def parse_sheet3(df, company_name: str, company_scale: str, checked_options: Dict[int, List[str]] = None) -> Dict:
    """
    解析Sheet 3: 综合招聘效能提升
    返回结构化的问卷响应数据
    """
    responses = {}
    checked_options = checked_options or {}
    
    for i in range(len(df)):
        row = df.iloc[i].values
        if len(row) < 3:
            continue
        
        # 找到问题行：第3列(index 2)包含问题编号如 "1.1 我们内部是否..."
        q_text = str(row[2]).strip() if pd.notna(row[2]) else ''
        
        # Match question number pattern
        for q_id in SHEET3_QUESTIONS:
            if q_text.startswith(q_id + ' ') or q_text.startswith(q_id + '\t'):
                answers = checked_options.get(i) or _extract_answer(row)
                if answers:
                    responses[q_id] = {
                        'answers': answers,
                        'company': company_name,
                        'scale': company_scale,
                    }
                break
    
    return responses


def parse_sheet4(df, company_name: str, company_scale: str) -> Dict:
    """
    解析Sheet 4: 热点职位调研
    返回: {
        'hc_forecast': {职能: 预测HC数}, 
        'hot_functions': [热点职能列表]
    }
    """
    result = {'hc_forecast': {}, 'hot_functions': [], 'company': company_name, 'scale': company_scale}
    
    func_map = {
        '1.1': '公司整体',
        '1.2': '早期研发',
        '1.3': '临床开发',
        '1.4': '商业',
        '1.5': '生产及供应链',
        '1.6': '职能',
    }
    
    for i in range(len(df)):
        row = df.iloc[i].values
        if len(row) < 3:
            continue
        
        row_id = str(row[0]).strip() if pd.notna(row[0]) else ''
        
        # HC forecast
        if row_id in func_map:
            func_name = func_map[row_id]
            # Column C (index 2) = 2026 new HC count
            hc = row[2] if len(row) > 2 and pd.notna(row[2]) else None
            if hc is not None:
                try:
                    hc_val = float(str(hc).replace(',', ''))
                    result['hc_forecast'][func_name] = hc_val
                except (ValueError, TypeError):
                    pass
            
            # Hot functions in columns D, E, F (index 3, 4, 5)
            for col_idx in [3, 4, 5]:
                if col_idx < len(row) and pd.notna(row[col_idx]):
                    hot = str(row[col_idx]).strip()
                    if hot and hot not in ['热点职能 1', '热点职能 2', '热点职能 3', '待补充']:
                        result['hot_functions'].append(hot)
    
    return result


def parse_sheet5(df, company_name: str, company_scale: str, checked_options: Dict[int, List[str]] = None) -> Dict:
    """
    解析Sheet 5: 高管任期变化趋势调研
    返回问题-答案对
    """
    responses = {}
    checked_options = checked_options or {}
    
    for i in range(len(df)):
        row = df.iloc[i].values
        if len(row) < 4:
            continue
        
        # 问题编号在第2列(index 1)的子编号, 如 "1.1", "2.1"
        q_ref = str(row[2]).strip() if pd.notna(row[2]) else ''
        
        for q_id in SHEET5_QUESTIONS:
            if q_ref.startswith(q_id + ' ') or q_ref.startswith(q_id + '\t') or q_ref == q_id:
                answers = checked_options.get(i) or _extract_answer(row, start_col=3)
                # Also check for text answers in specific columns
                if not answers and len(row) > 3 and pd.notna(row[3]):
                    text_val = str(row[3]).strip()
                    if text_val and '填写' not in text_val:
                        answers = [text_val]
                
                if answers:
                    responses[q_id] = {
                        'answers': answers,
                        'company': company_name,
                        'scale': company_scale,
                    }
                break
    
    return responses


# ============================================================
# 行业聚合分析
# ============================================================

class SurveyAggregator:
    """
    聚合多家公司的问卷调研数据，生成行业趋势分析
    """
    
    def __init__(self):
        self.sheet3_responses = []  # List of {q_id: {answers, company, scale}}
        self.sheet4_data = []       # List of {hc_forecast, hot_functions, company, scale}
        self.sheet5_responses = []  # List of {q_id: {answers, company, scale}}
        self.companies = []
    
    def add_company(self, company_name: str, company_scale: str,
                    sheet3: Dict = None, sheet4: Dict = None, sheet5: Dict = None):
        """添加一家公司的调研数据"""
        if not any(c.get('name') == company_name for c in self.companies):
            self.companies.append({'name': company_name, 'scale': company_scale})
        if sheet3:
            self.sheet3_responses.append(sheet3)
        if sheet4:
            self.sheet4_data.append(sheet4)
        if sheet5:
            self.sheet5_responses.append(sheet5)
    
    def aggregate_sheet3(self) -> Dict:
        """
        聚合Sheet 3数据，计算各选项的选择比例
        返回: {q_id: {
            '整体': {option: percentage}, 
            'A': {option: percentage}, 
            'B': {option: percentage}
        }}
        """
        result = {}
        
        for q_id, (module, question, q_type, options) in SHEET3_QUESTIONS.items():
            q_result = {'module': module, 'question': question, 'type': q_type}
            
            for group_label, filter_fn in [
                ('整体', lambda s: True),
                ('A', lambda s: s == 'A'),
                ('B', lambda s: s == 'B'),
            ]:
                answer_counter = Counter()
                total_respondents = 0
                
                for company_resp in self.sheet3_responses:
                    if q_id in company_resp:
                        resp = company_resp[q_id]
                        if filter_fn(resp['scale']):
                            total_respondents += 1
                            for ans in resp['answers']:
                                answer_counter[ans] += 1
                
                if total_respondents > 0:
                    total_answers = sum(answer_counter.values())
                    distribution = {}
                    for ans, count in answer_counter.most_common():
                        distribution[ans] = count / total_answers if total_answers > 0 else 0
                    q_result[group_label] = {
                        'distribution': distribution,
                        'respondents': total_respondents,
                        'total_answers': total_answers,
                    }
                else:
                    q_result[group_label] = {'distribution': {}, 'respondents': 0, 'total_answers': 0}
            
            result[q_id] = q_result
        
        return result
    
    def aggregate_sheet4(self) -> Dict:
        """
        聚合Sheet 4数据
        返回: {
            'hc_forecast': {职能: {P25, P50, P75, mean}},
            'hot_functions': Counter of hot functions
        }
        """
        result = {'hc_forecast': {}, 'hot_functions': Counter()}
        
        # HC forecast aggregation
        func_hc = defaultdict(list)
        for data in self.sheet4_data:
            for func, hc in data.get('hc_forecast', {}).items():
                if hc > 0:
                    func_hc[func].append(hc)
        
        for func, values in func_hc.items():
            arr = np.array(values)
            result['hc_forecast'][func] = {
                'P25': np.percentile(arr, 25),
                'P50': np.median(arr),
                'P75': np.percentile(arr, 75),
                'mean': np.mean(arr),
                'count': len(arr),
            }
        
        # Hot functions
        for data in self.sheet4_data:
            for func in data.get('hot_functions', []):
                result['hot_functions'][func] += 1
        
        return result
    
    def aggregate_sheet5(self) -> Dict:
        """
        聚合Sheet 5数据
        返回: {q_id: {整体/A/B: {distribution, respondents}}}
        """
        result = {}
        
        for q_id, (module, question, q_type, options) in SHEET5_QUESTIONS.items():
            q_result = {'module': module, 'question': question, 'type': q_type}
            
            for group_label, filter_fn in [
                ('整体', lambda s: True),
                ('A', lambda s: s == 'A'),
                ('B', lambda s: s == 'B'),
            ]:
                answer_counter = Counter()
                text_answers = []
                total_respondents = 0
                
                for company_resp in self.sheet5_responses:
                    if q_id in company_resp:
                        resp = company_resp[q_id]
                        if filter_fn(resp['scale']):
                            total_respondents += 1
                            for ans in resp['answers']:
                                if q_type == 'text':
                                    text_answers.append(ans)
                                else:
                                    answer_counter[ans] += 1
                
                if total_respondents > 0:
                    total_answers = sum(answer_counter.values())
                    distribution = {}
                    for ans, count in answer_counter.most_common():
                        distribution[ans] = count / total_answers if total_answers > 0 else 0
                    q_result[group_label] = {
                        'distribution': distribution,
                        'respondents': total_respondents,
                        'total_answers': total_answers,
                        'text_answers': text_answers,
                    }
                else:
                    q_result[group_label] = {'distribution': {}, 'respondents': 0, 'total_answers': 0, 'text_answers': []}
            
            result[q_id] = q_result
        
        return result


def _question_id(text) -> Optional[str]:
    match = re.match(r'\s*(\d+\.\d+)', str(text or ''))
    return match.group(1) if match else None


def load_compiled_survey_summaries(data_dir: str, aggregator: SurveyAggregator = None) -> SurveyAggregator:
    """
    Load pre-compiled summary workbooks for survey Sheets 3/4.

    These files preserve form-control answers that are hard to read from raw Excel
    unless VML controls are available.
    """
    agg = aggregator or SurveyAggregator()

    sheet3_path = os.path.join(data_dir, '4.7 综合招聘效能提升汇总表.xlsx')
    if os.path.exists(sheet3_path):
        df = pd.read_excel(sheet3_path)
        for company, sub in df.groupby('所属公司', dropna=True):
            scale_vals = sub['公司规模'].dropna().astype(str).str.strip()
            scale = scale_vals.iloc[0] if not scale_vals.empty else 'B'
            responses = {}
            for _, row in sub.iterrows():
                q_id = _question_id(row.get('题目'))
                if not q_id:
                    continue
                answers = []
                for letter in list('ABCDEFG'):
                    col = f'选择{letter}'
                    if col in sub.columns and pd.notna(row.get(col)) and float(row.get(col) or 0) > 0:
                        answers.append(letter)
                if answers:
                    responses[q_id] = {'answers': answers, 'company': company, 'scale': scale}
            agg.add_company(str(company), scale, sheet3=responses)

    sheet4_path = os.path.join(data_dir, '4.9 2025财年热点职能汇总表.xlsx')
    if os.path.exists(sheet4_path):
        df = pd.read_excel(sheet4_path)
        for company, sub in df.groupby('所属公司', dropna=True):
            scale_vals = sub['公司规模'].dropna().astype(str).str.strip()
            scale = scale_vals.iloc[0] if not scale_vals.empty else 'B'
            hot = []
            for col in ['热点职能 1', '热点职能 2', '热点职能 3']:
                if col in sub.columns:
                    hot.extend([str(v).strip() for v in sub[col].dropna() if str(v).strip()])
            agg.add_company(str(company), scale, sheet4={'hot_functions': hot, 'hc_forecast': {}, 'company': company, 'scale': scale})

    focus_path = os.path.join(data_dir, '4.10 重点关注职能汇总表.xlsx')
    if os.path.exists(focus_path):
        df = pd.read_excel(focus_path)
        for _, row in df.iterrows():
            company = str(row.get('所属公司', '')).strip()
            if not company:
                continue
            scale = str(row.get('公司规模', 'B')).strip()
            hot = []
            for col in df.columns[2:]:
                val = row.get(col)
                if pd.notna(val) and str(val).strip() and str(val).strip() not in ('0', 'nan'):
                    hot.append(str(col).strip())
            if hot:
                agg.add_company(company, scale, sheet4={'hot_functions': hot, 'hc_forecast': {}, 'company': company, 'scale': scale})

    return agg


# ============================================================
# 报告生成
# ============================================================

class SurveyReportGenerator:
    """生成调研趋势分析报告"""
    
    def __init__(self, aggregator: SurveyAggregator):
        self.agg = aggregator
    
    def generate_full_report(self) -> str:
        """生成完整的调研趋势分析报告"""
        lines = []
        lines.append("# TA招聘实践调研报告\n")
        lines.append(f"参调公司: {len(self.agg.companies)}家\n")
        
        # Part 1: Sheet 3 - 招聘实践
        lines.append("\n---\n## Part 1: 综合招聘效能提升\n")
        lines.extend(self._report_sheet3())
        
        # Part 2: Sheet 4 - 热点职位
        lines.append("\n---\n## Part 2: 2026年新增HC预测与热点岗位\n")
        lines.extend(self._report_sheet4())
        
        # Part 3: Sheet 5 - 高管任期
        lines.append("\n---\n## Part 3: 高管任期变化趋势调研\n")
        lines.extend(self._report_sheet5())
        
        return "\n".join(lines)

    def _methodology_note(self, curr_year: str, prev_year: str) -> Tuple[str, str, str]:
        try:
            publish_year = str(int(curr_year) + 1)
            reference_publish_year = str(int(prev_year) + 1)
        except (TypeError, ValueError):
            publish_year = "下一年度"
            reference_publish_year = "历史"
        note = (
            f"{prev_year}年数据/{reference_publish_year}年发布PDF仅用于参考分析维度和呈现口径，"
            "不参与任何比例、样本量或结论计算。"
        )
        return publish_year, reference_publish_year, note

    def generate_sheet4_outlook_report(self, curr_year: str = "2025", prev_year: str = "2024") -> str:
        """Generate Sheet 4 as a standalone forward-looking outlook section."""
        publish_year, _, note = self._methodology_note(curr_year, prev_year)
        lines = []
        lines.append(f"### {publish_year}新增HC预测与热点岗位前瞻（Sheet 4）\n")
        lines.append(f"数据来源: 仅使用{curr_year}年调研问卷（{len(self.agg.companies)}家参调公司）")
        lines.append(f"\n> {note} Sheet 4 用于前置呈现{publish_year}年新增HC和热点岗位方向。")
        lines.append("")
        lines.extend(self._compact_sheet4())
        return "\n".join(lines)

    def generate_sheet3_practice_report(self, curr_year: str = "2025", prev_year: str = "2024") -> str:
        """Generate Sheet 3 as a standalone TA practice trend section."""
        publish_year, _, note = self._methodology_note(curr_year, prev_year)
        lines = []
        lines.append(f"### {publish_year}报告TA招聘实践趋势分析（Sheet 3，基于{curr_year}年调研问卷）\n")
        lines.append(f"数据来源: 仅使用{curr_year}年调研问卷（{len(self.agg.companies)}家参调公司）")
        lines.append(f"\n> {note} 参考维度包括：岗位招聘策略、候选人识别、Offer接受、雇主品牌、候选人体验、招聘流程前置介入与权限边界。")

        lines.append("\n#### 1. 招聘策略与候选人识别\n")
        lines.extend(self._compact_questions(['2.3', '2.1', '2.2']))

        lines.append("\n#### 2. 候选人体验、Offer接受与雇主品牌\n")
        lines.extend(self._compact_questions(['6.1', '1.1', '1.4', '4.1']))

        lines.append("\n#### 3. 招聘流程前置介入与权限边界\n")
        lines.extend(self._compact_questions(['3.2', '3.3', '3.5']))
        return "\n".join(lines)

    def generate_sheet5_tenure_report(self, curr_year: str = "2025", prev_year: str = "2024") -> str:
        """Generate Sheet 5 as a standalone executive tenure trend section."""
        publish_year, _, note = self._methodology_note(curr_year, prev_year)
        lines = []
        lines.append(f"### {publish_year}报告高管任期变化趋势分析（Sheet 5）\n")
        lines.append(f"数据来源: 仅使用{curr_year}年调研问卷（{len(self.agg.companies)}家参调公司）")
        lines.append(f"\n> {note} Sheet 5 独立呈现高管任期缩短、驱动因素、组织影响与企业应对。")
        lines.append("")
        lines.extend(self._compact_sheet5())
        return "\n".join(lines)

    def generate_split_reports(self, curr_year: str = "2025", prev_year: str = "2024") -> Dict[str, str]:
        """Return separated Sheet 4 / Sheet 3 / Sheet 5 report sections."""
        return {
            'sheet4': self.generate_sheet4_outlook_report(curr_year, prev_year),
            'sheet3': self.generate_sheet3_practice_report(curr_year, prev_year),
            'sheet5': self.generate_sheet5_tenure_report(curr_year, prev_year),
        }

    def generate_pdf_aligned_trend_report(self, curr_year: str = "2025", prev_year: str = "2024") -> str:
        """Backward-compatible wrapper: Sheet 3 practice section only."""
        return self.generate_sheet3_practice_report(curr_year, prev_year)

    def _option_label(self, q_id: str, opt: str, sheet5: bool = False) -> str:
        question_bank = SHEET5_QUESTIONS if sheet5 else SHEET3_QUESTIONS
        options = question_bank.get(q_id, ('', '', '', []))[3]
        idx = ord(str(opt)[0]) - ord('A') if str(opt) else -1
        if 0 <= idx < len(options):
            return options[idx]
        return str(opt)

    def _compact_questions(self, q_ids: List[str]) -> List[str]:
        lines = []
        data = self.agg.aggregate_sheet3()
        for q_id in q_ids:
            q_data = data.get(q_id)
            if not q_data or q_data.get('整体', {}).get('respondents', 0) == 0:
                lines.append(f"- **{q_id}**: 暂无有效样本。")
                continue
            group = q_data['整体']
            dist = group.get('distribution', {})
            top = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:3]
            top_text = "；".join(f"{self._option_label(q_id, opt)} {pct:.1%}" for opt, pct in top)
            a_resp = q_data.get('A', {}).get('respondents', 0)
            b_resp = q_data.get('B', {}).get('respondents', 0)
            lines.append(f"- **{q_id} {q_data['question']}**：{top_text}。样本：整体{group['respondents']}家，A类{a_resp}家，B类{b_resp}家。")
        return lines

    def _compact_sheet4(self) -> List[str]:
        lines = []
        data = self.agg.aggregate_sheet4()
        if data.get('hc_forecast'):
            lines.append("| 职能 | P25 | P50 | P75 | 样本数 |")
            lines.append("|------|------|------|------|------|")
            for func, d in data['hc_forecast'].items():
                lines.append(f"| {func} | {d['P25']:.0f} | {d['P50']:.0f} | {d['P75']:.0f} | {d['count']} |")
        if data.get('hot_functions'):
            total = len(self.agg.sheet4_data) or 1
            lines.append("\n**热点岗位/职能关注度 Top 10**")
            for rank, (func, count) in enumerate(data['hot_functions'].most_common(10), 1):
                lines.append(f"{rank}. {func}: {count}家公司关注（{count/total:.0%}）")
        if not data.get('hc_forecast') and not data.get('hot_functions'):
            lines.append("*暂无 Sheet 4 有效样本。*")
        return lines

    def _compact_sheet5(self) -> List[str]:
        lines = []
        data = self.agg.aggregate_sheet5()
        # Show all closed-ended Sheet 5 questions in the compact report.
        # Open-ended items (1.4 / 5.4) stay in the audit table, not the summary.
        key_questions = ['1.1', '1.2', '1.3', '2.1', '2.2', '3.1', '3.2', '4.1', '4.2', '5.1', '5.2', '5.3', '5.5']
        for q_id in key_questions:
            q_data = data.get(q_id)
            if not q_data or q_data.get('整体', {}).get('respondents', 0) == 0:
                if q_id == '3.2':
                    lines.append("- **3.2 组织与人才影响评估补充题**：上传问卷中未发现独立3.2题号，暂无有效样本。")
                continue
            group = q_data['整体']
            dist = group.get('distribution', {})
            if dist:
                top = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:3]
                top_text = "；".join(f"{self._option_label(q_id, opt, sheet5=True)} {pct:.1%}" for opt, pct in top)
                lines.append(f"- **{q_id} {q_data['question']}**：{top_text}。样本：{group['respondents']}家。")
            elif group.get('text_answers'):
                examples = "；".join(group['text_answers'][:3])
                lines.append(f"- **{q_id} {q_data['question']}**：开放反馈示例：{examples}")
        if not lines:
            lines.append("*暂无 Sheet 5 有效样本。*")
        return lines
    
    def _report_sheet3(self) -> List[str]:
        """生成Sheet 3报告"""
        lines = []
        data = self.agg.aggregate_sheet3()
        
        # Group by module
        modules = {}
        for q_id, q_data in data.items():
            mod = q_data['module']
            if mod not in modules:
                modules[mod] = []
            modules[mod].append((q_id, q_data))
        
        module_names_cn = {
            'Employer Branding': '1. Employer Branding（雇主品牌塑造）',
            'Talent Acquisition Strategy': '2. Talent Acquisition Strategy（人才获取策略）',
            'Recruitment Process Optimization': '3. Recruitment Process Optimization（招聘流程优化）',
            'Candidate Experience Management': '4. Candidate Experience Management（候选人体验管理）',
            'Performance Management': '5. Performance Management（绩效管理）',
            'Talent Retention and Development': '6. Talent Retention and Development（人才留存与发展）',
            'Hitting Rate': '7. Hitting Rate（招聘命中率）',
            'Cost for per hiring': '8. Cost for per hiring（每次招聘成本）',
        }
        
        for mod_key, mod_label in module_names_cn.items():
            if mod_key not in modules:
                continue
            lines.append(f"\n### {mod_label}\n")
            
            for q_id, q_data in modules[mod_key]:
                q_text = q_data['question']
                lines.append(f"\n**{q_id} {q_text}**\n")
                
                # Distribution table
                all_group = q_data.get('整体', {})
                a_group = q_data.get('A', {})
                b_group = q_data.get('B', {})
                
                if all_group.get('respondents', 0) == 0:
                    lines.append("*无响应数据*\n")
                    continue
                
                # Collect all options
                all_options = set()
                for g in [all_group, a_group, b_group]:
                    all_options.update(g.get('distribution', {}).keys())
                
                if all_options:
                    lines.append(f"| 选项 | 整体 | A类 | B类 |")
                    lines.append(f"|------|------|------|------|")
                    for opt in sorted(all_options):
                        v_all = all_group.get('distribution', {}).get(opt, 0)
                        v_a = a_group.get('distribution', {}).get(opt, 0)
                        v_b = b_group.get('distribution', {}).get(opt, 0)
                        lines.append(f"| {opt} | {v_all:.1%} | {v_a:.1%} | {v_b:.1%} |")
                    
                    lines.append(f"\n*样本: 整体{all_group['respondents']}家, A类{a_group.get('respondents',0)}家, B类{b_group.get('respondents',0)}家*\n")
        
        return lines
    
    def _report_sheet4(self) -> List[str]:
        """生成Sheet 4报告"""
        lines = []
        data = self.agg.aggregate_sheet4()
        
        # HC Forecast
        if data['hc_forecast']:
            lines.append("### 2026年各职能新增HC预测\n")
            lines.append("| 职能 | P25 | P50 | P75 | 均值 | 样本数 |")
            lines.append("|------|------|------|------|------|------|")
            for func in ['公司整体', '早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                if func in data['hc_forecast']:
                    d = data['hc_forecast'][func]
                    lines.append(f"| {func} | {d['P25']:.0f} | {d['P50']:.0f} | {d['P75']:.0f} | {d['mean']:.0f} | {d['count']} |")
        
        # Hot Functions
        if data['hot_functions']:
            lines.append("\n### 2026年热点职能关注度排行\n")
            lines.append("| 排名 | 热点职能 | 关注公司数 | 关注比例 |")
            lines.append("|------|----------|----------|----------|")
            total = len(self.agg.sheet4_data) or 1
            for rank, (func, count) in enumerate(data['hot_functions'].most_common(15), 1):
                lines.append(f"| {rank} | {func} | {count} | {count/total:.0%} |")
        
        return lines
    
    def _report_sheet5(self) -> List[str]:
        """生成Sheet 5报告"""
        lines = []
        data = self.agg.aggregate_sheet5()
        
        if not data:
            lines.append("*无高管任期调研数据*")
            return lines
        
        # Group by module
        modules = {}
        for q_id, q_data in data.items():
            mod = q_data['module']
            if mod not in modules:
                modules[mod] = []
            modules[mod].append((q_id, q_data))
        
        for mod_name, questions in modules.items():
            lines.append(f"\n### {mod_name}\n")
            
            for q_id, q_data in questions:
                q_text = q_data['question']
                lines.append(f"\n**{q_id} {q_text}**\n")
                
                all_group = q_data.get('整体', {})
                if all_group.get('respondents', 0) == 0:
                    lines.append("*无响应数据*\n")
                    continue
                
                # Text answers
                if q_data['type'] == 'text':
                    text_answers = all_group.get('text_answers', [])
                    if text_answers:
                        for ans in text_answers:
                            lines.append(f"- {ans}")
                    continue
                
                # Distribution
                all_options = set()
                a_group = q_data.get('A', {})
                b_group = q_data.get('B', {})
                for g in [all_group, a_group, b_group]:
                    all_options.update(g.get('distribution', {}).keys())
                
                if all_options:
                    lines.append(f"| 选项 | 整体 | A类 | B类 |")
                    lines.append(f"|------|------|------|------|")
                    for opt in sorted(all_options):
                        v_all = all_group.get('distribution', {}).get(opt, 0)
                        v_a = a_group.get('distribution', {}).get(opt, 0)
                        v_b = b_group.get('distribution', {}).get(opt, 0)
                        lines.append(f"| {opt} | {v_all:.1%} | {v_a:.1%} | {v_b:.1%} |")
                    
                    lines.append(f"\n*样本: 整体{all_group['respondents']}家*\n")
        
        return lines


# ============================================================
# 公司问卷批量摄入
# ============================================================

def ingest_surveys(filepath: str, company_name: str, company_scale: str) -> Dict:
    """
    从一个公司问卷中摄入Sheets 3/4/5数据
    Returns: {'sheet3': dict, 'sheet4': dict, 'sheet5': dict}
    """
    result = {}
    
    try:
        xl = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"[WARN] Cannot open {filepath}: {e}")
        return result
    
    # Sheet 3
    sheet3_name = _find_sheet(xl.sheet_names, '综合招聘效能')
    if sheet3_name:
        try:
            df3 = pd.read_excel(filepath, sheet_name=sheet3_name, header=None)
            checked = _extract_vml_checked_options(filepath, sheet3_name)
            result['sheet3'] = parse_sheet3(df3, company_name, company_scale, checked)
        except Exception as e:
            print(f"[WARN] Sheet 3 parse error for {company_name}: {e}")
    
    # Sheet 4
    sheet4_name = _find_sheet(xl.sheet_names, '热点职位')
    if sheet4_name:
        try:
            df4 = pd.read_excel(filepath, sheet_name=sheet4_name, header=None)
            result['sheet4'] = parse_sheet4(df4, company_name, company_scale)
        except Exception as e:
            print(f"[WARN] Sheet 4 parse error for {company_name}: {e}")
    
    # Sheet 5
    sheet5_name = _find_sheet(xl.sheet_names, '高管任期')
    if sheet5_name:
        try:
            df5 = pd.read_excel(filepath, sheet_name=sheet5_name, header=None)
            checked = _extract_vml_checked_options(filepath, sheet5_name, prefer_labels=True)
            result['sheet5'] = parse_sheet5(df5, company_name, company_scale, checked)
        except Exception as e:
            print(f"[WARN] Sheet 5 parse error for {company_name}: {e}")
    
    return result


# ============================================================
# 测试
# ============================================================
if __name__ == '__main__':
    import sys
    test_file = sys.argv[1] if len(sys.argv) > 1 else r'D:\win设备桌面\2025年业绩核算\TA效能报告\数据源\各家数据\TA 效能\TA效能数据2025\BeOne.xlsx'
    
    if os.path.exists(test_file):
        result = ingest_surveys(test_file, '百济神州', 'A')
        print(f"\n=== Sheet 3 responses: {len(result.get('sheet3', {}))} questions ===")
        for q_id, resp in sorted(result.get('sheet3', {}).items()):
            print(f"  {q_id}: {resp['answers']}")
        
        print(f"\n=== Sheet 4 data ===")
        s4 = result.get('sheet4', {})
        print(f"  HC forecast: {s4.get('hc_forecast', {})}")
        print(f"  Hot functions: {s4.get('hot_functions', [])}")
        
        print(f"\n=== Sheet 5 responses: {len(result.get('sheet5', {}))} questions ===")
        for q_id, resp in sorted(result.get('sheet5', {}).items()):
            print(f"  {q_id}: {resp['answers']}")
    else:
        print(f"File not found: {test_file}")
