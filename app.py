"""
TA效能报告 - LLM Wiki 自动化分析工具
Streamlit 前端界面 - 支持单公司/多公司行业报告
"""
import os, sys, json, tempfile, datetime, importlib, re
from typing import Optional
import pandas as pd
import numpy as np
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_single_company import (
    _find_sheet, clean_value, compile_to_wiki, generate_report, validate_data
)
from multi_company import (
    ingest_company, IndustryAggregator, IndustryReportGenerator,
    generate_audit_report, _external_cost_hire_count
)
from data_validator import DataValidator, validate_and_clean, generate_validation_report, ValidationResult
from wiki.knowledge_base import KnowledgeBase, KnowledgeEntry
import yoy_comparator as yoy_comparator_module
yoy_comparator_module = importlib.reload(yoy_comparator_module)
YoYComparator = yoy_comparator_module.YoYComparator
YoYReportComparator = yoy_comparator_module.YoYReportComparator
import report_parser as report_parser_module
report_parser_module = importlib.reload(report_parser_module)
parse_published_report = report_parser_module.parse_published_report
parse_published_reports = report_parser_module.parse_published_reports
PriorYearData = report_parser_module.PriorYearData
import survey_parser as survey_parser_module
survey_parser_module = importlib.reload(survey_parser_module)
SurveyAggregator = survey_parser_module.SurveyAggregator
SurveyReportGenerator = survey_parser_module.SurveyReportGenerator
ingest_surveys = survey_parser_module.ingest_surveys

st.set_page_config(page_title="TA效能分析工具", page_icon="📊", layout="wide")

st.markdown("""<style>
.metric-card{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
padding:1.2rem;border-radius:10px;color:white;text-align:center;margin-bottom:.5rem}
.metric-card .val{font-size:1.8rem;font-weight:bold}
.metric-card .lbl{font-size:.85rem;opacity:.9}
</style>""", unsafe_allow_html=True)


def card(label, value):
    st.markdown(f'<div class="metric-card"><div class="lbl">{label}</div><div class="val">{value}</div></div>',
                unsafe_allow_html=True)


def _company_name_scale(raw: dict, fallback_name: str = "未知公司") -> tuple:
    info = raw.get('company_info', {}) if isinstance(raw, dict) else {}
    name = (
        info.get('公司名称') or info.get('company_name') or
        info.get('公司') or fallback_name
    )
    scale = (
        info.get('公司规模分类') or info.get('公司规模') or
        info.get('scale') or 'B'
    )
    scale = str(scale).strip().upper()
    if scale not in ('A', 'B'):
        scale = 'B'
    return str(name).strip() or fallback_name, scale


def _fill_company_name_from_upload(raw: dict, filename: str) -> None:
    """Use the upload filename when the questionnaire does not expose company name."""
    info = raw.setdefault('company_info', {}) if isinstance(raw, dict) else {}
    current = str(info.get('公司名称') or info.get('company_name') or '').strip()
    if current and current.lower() not in {'未知', '未知公司', 'nan', 'none'}:
        return
    base = os.path.splitext(os.path.basename(filename))[0].strip()
    base = re.sub(r'^\s*20\d{2}\s*TA\s*问卷[_ -]*医药行业版本[-_ ]*', '', base, flags=re.I)
    base = re.sub(r'^\s*20\d{2}\s*TA\s*', '', base, flags=re.I)
    base = re.sub(r'^\s*20\d{2}\s*', '', base, flags=re.I)
    info['公司名称'] = base or filename


def _add_survey_from_file(survey_agg: SurveyAggregator, filepath: str, raw: dict, fallback_name: str) -> Optional[str]:
    try:
        company, scale = _company_name_scale(raw, fallback_name)
        survey = ingest_surveys(filepath, company, scale)
        if survey.get('sheet3') or survey.get('sheet4') or survey.get('sheet5'):
            survey_agg.add_company(
                company, scale,
                sheet3=survey.get('sheet3'),
                sheet4=survey.get('sheet4'),
                sheet5=survey.get('sheet5'),
            )
    except Exception as exc:
        return f"{fallback_name}: 问卷Sheets 3/4/5解析失败: {exc}"
    return None


def _survey_trend_text(survey_agg: SurveyAggregator, curr_year: str, prev_year: str) -> dict:
    if not (survey_agg.sheet3_responses or survey_agg.sheet4_data or survey_agg.sheet5_responses):
        return {}
    return SurveyReportGenerator(survey_agg).generate_split_reports(
        curr_year=curr_year, prev_year=prev_year
    )


# ==================== 侧边栏 ====================
with st.sidebar:
    st.markdown("## 📊 TA效能分析工具")
    st.markdown("---")
    mode = st.radio("分析模式", ["🏢 单公司分析", "🏭 行业报告（多公司）", "📈 年度对比分析"], index=1)
    st.markdown("---")
    st.markdown("### 📋 使用步骤")
    if "年度对比" in mode:
        st.markdown("1. 上传本年度问卷\n2. 上传上年度**最终口径表**或问卷\n3. 自动比对分析\n4. 生成趋势报告\n5. 下载结果")
    elif "行业" in mode:
        st.markdown("1. 上传多家公司问卷\n2. 系统批量解析\n3. 生成行业P50报告\n4. 下载结果")
    else:
        st.markdown("1. 上传单家问卷\n2. 自动解析数据\n3. 查看分析报告\n4. 下载结果")
    st.markdown("---")
    st.caption("v2.1-fixed · 固定口径：一级渠道/外部渠道细分/同公司闭合校验")

# ==================== 主页面 ====================
st.markdown("# 📊 TA效能报告 - 自动化分析工具")

if "年度对比" in mode:
    # ==================== 年度对比分析模式 ====================
    st.markdown("### 📈 年度对比分析 — 上传两年数据，生成趋势报告")

    # 选择上年度数据来源模式
    yoy_mode = st.radio(
        "上年度数据来源",
        ["📊 最终口径表（推荐）", "📋 调研问卷"],
        index=0,
        horizontal=True,
        help="推荐使用上一年度最终报告口径表：直接引用已发布分析结果，比PDF/PPTX或中间总表抽数更稳定"
    )

    use_report_mode = "最终口径表" in yoy_mode

    if use_report_mode:
        # ==================== 报告模式 ====================
        st.markdown("当年上传**调研问卷**，上年度上传**最终报告口径表**(XLSX/XLS)，直接引用已发布分析结果。")

        col_curr, col_prev = st.columns(2)
        with col_curr:
            st.markdown("#### 📂 本年度调研数据")
            curr_year = st.text_input("本年度", value="2025", key="curr_year_rpt")
            curr_files = st.file_uploader(
                f"上传 {curr_year} 年调研问卷",
                type=['xlsx', 'xls'], accept_multiple_files=True, key="curr_files_rpt"
            )
            if curr_files:
                st.success(f"已选择 {len(curr_files)} 个问卷文件")

        with col_prev:
            st.markdown("#### 📊 上年度最终口径表")
            prev_year = st.text_input("上年度", value="2024", key="prev_year_rpt")
            prev_report_file = st.file_uploader(
                f"上传 {prev_year} 年最终口径表",
                type=['xlsx', 'xls'], accept_multiple_files=True, key="prev_report_file"
            )
            if prev_report_file:
                st.success(f"✅ 已选择 {len(prev_report_file)} 个口径表文件")
                st.caption("优先上传最终报告口径表；仍支持多份 XLSX / XLS 作为备用")

        can_run = curr_files and prev_report_file
        if can_run:
            if st.button("🚀 开始年度对比分析（报告模式）", type="primary", use_container_width=True):
                progress = st.progress(0, text="准备中...")

                # Phase 1: 摄入本年度问卷
                curr_agg = IndustryAggregator()
                survey_agg = SurveyAggregator()
                raw_list = []
                curr_errs = []
                tmp_files = []
                for i, f in enumerate(curr_files):
                    progress.progress(
                        (i + 1) / (len(curr_files) + 4),
                        text=f"📥 摄入{curr_year}年 ({i+1}/{len(curr_files)}): {f.name}"
                    )
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                            tmp.write(f.getvalue())
                            tmp_path = tmp.name
                        tmp_files.append(tmp_path)
                        raw = ingest_company(tmp_path)
                        _fill_company_name_from_upload(raw, f.name)
                        raw_list.append(raw)
                        survey_err = _add_survey_from_file(survey_agg, tmp_path, raw, f.name)
                        if survey_err:
                            curr_errs.append(survey_err)
                    except Exception as e:
                        curr_errs.append(f"{f.name}: {e}")
                # Cleanup temp files
                for tp in tmp_files:
                    try: os.unlink(tp)
                    except: pass

                cleaned, _, _ = validate_and_clean(raw_list)
                for data in cleaned:
                    curr_agg.add_company(data)

                # Phase 2: 解析上年度最终口径表
                progress.progress(0.6, text=f"📊 解析{prev_year}年最终口径表...")
                prev_tmp_paths = []
                try:
                    for f in prev_report_file:
                        ext = os.path.splitext(f.name)[1].lower()
                        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                            tmp.write(f.getvalue())
                            prev_tmp_paths.append(tmp.name)
                    prev_data = parse_published_reports(prev_tmp_paths, year=prev_year)
                except Exception as e:
                    st.error(f"上年度最终口径表解析失败: {e}")
                    prev_data = None
                finally:
                    for tp in prev_tmp_paths:
                        try: os.unlink(tp)
                        except: pass

                if prev_data:
                    # Phase 3: 生成本年度行业报告
                    progress.progress(0.75, text="📊 生成本年度行业报告...")
                    curr_gen = IndustryReportGenerator(curr_agg)
                    curr_report = curr_gen.generate_full_report()

                    # Phase 4: 生成年度对比报告（报告模式）
                    progress.progress(0.9, text="📈 生成年度对比趋势报告...")
                    survey_trend_report = _survey_trend_text(survey_agg, curr_year, prev_year)
                    comparator = YoYReportComparator(
                        curr_agg, prev_data,
                        curr_year=curr_year, prev_year=prev_year,
                        survey_trend_report=survey_trend_report
                    )
                    yoy_report = comparator.generate_yoy_report()
                    yoy_table = comparator.export_comparison_table()

                    progress.progress(1.0, text="✅ 年度对比分析完成!")

                    st.session_state['yoy_curr_agg'] = curr_agg
                    st.session_state['yoy_prev_data'] = prev_data
                    st.session_state['yoy_curr_report'] = curr_report
                    st.session_state['yoy_report'] = yoy_report
                    st.session_state['survey_trend_report'] = survey_trend_report
                    st.session_state['yoy_table'] = yoy_table
                    st.session_state['yoy_comparator'] = comparator
                    st.session_state['yoy_curr_year'] = curr_year
                    st.session_state['yoy_prev_year'] = prev_year
                    st.session_state['yoy_mode'] = 'report'
                    if curr_errs:
                        st.session_state['yoy_errors'] = curr_errs

    else:
        # ==================== 问卷模式（原有逻辑） ====================
        st.markdown("分别上传**本年度**和**上年度**的调研问卷，系统将自动比对所有维度指标变化趋势。")

        col_curr, col_prev = st.columns(2)
        with col_curr:
            st.markdown("#### 📂 本年度数据")
            curr_year = st.text_input("本年度", value="2024", key="curr_year")
            curr_files = st.file_uploader(
                f"上传 {curr_year} 年问卷",
                type=['xlsx', 'xls'], accept_multiple_files=True, key="curr_files"
            )
            if curr_files:
                st.success(f"已选择 {len(curr_files)} 个文件")

        with col_prev:
            st.markdown("#### 📂 上年度数据")
            prev_year = st.text_input("上年度", value="2023", key="prev_year")
            prev_files = st.file_uploader(
                f"上传 {prev_year} 年问卷",
                type=['xlsx', 'xls'], accept_multiple_files=True, key="prev_files"
            )
            if prev_files:
                st.success(f"已选择 {len(prev_files)} 个文件")

        can_run = curr_files and prev_files
        if can_run:
            if st.button("🚀 开始年度对比分析", type="primary", use_container_width=True):
                progress = st.progress(0, text="准备中...")

                def _ingest_batch(files, label):
                    """批量摄入一批问卷文件"""
                    agg = IndustryAggregator()
                    survey_agg = SurveyAggregator()
                    raw_list = []
                    errs = []
                    tmp_paths = []
                    for i, f in enumerate(files):
                        progress.progress(
                            (i + 1) / (len(files) * 2 + 4),
                            text=f"📥 摄入{label} ({i+1}/{len(files)}): {f.name}"
                        )
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                                tmp.write(f.getvalue())
                                tmp_path = tmp.name
                            tmp_paths.append(tmp_path)
                            raw = ingest_company(tmp_path)
                            _fill_company_name_from_upload(raw, f.name)
                            raw_list.append(raw)
                            survey_err = _add_survey_from_file(survey_agg, tmp_path, raw, f.name)
                            if survey_err:
                                errs.append(survey_err)
                        except Exception as e:
                            errs.append(f"{f.name}: {e}")
                    # Cleanup temp files after all ingestion done
                    for tp in tmp_paths:
                        try: os.unlink(tp)
                        except: pass
                    # Validate and add
                    cleaned, _, _ = validate_and_clean(raw_list)
                    for data in cleaned:
                        agg.add_company(data)
                    return agg, errs, survey_agg

                # Phase 1: 摄入本年度
                curr_agg, curr_errs, curr_survey_agg = _ingest_batch(curr_files, f"{curr_year}年")

                # Phase 2: 摄入上年度
                prev_agg, prev_errs, _ = _ingest_batch(prev_files, f"{prev_year}年")

                # Phase 3: 生成本年度行业报告（保留）
                progress.progress(0.7, text="📊 生成本年度行业报告...")
                curr_gen = IndustryReportGenerator(curr_agg)
                curr_report = curr_gen.generate_full_report()

                # Phase 4: 生成年度对比报告
                progress.progress(0.85, text="📈 生成年度对比趋势报告...")
                comparator = YoYComparator(curr_agg, prev_agg,
                                           curr_year=curr_year, prev_year=prev_year)
                yoy_report = comparator.generate_yoy_report()
                survey_trend_report = _survey_trend_text(curr_survey_agg, curr_year, prev_year)
                if survey_trend_report:
                    survey_parts = [
                        survey_trend_report.get('sheet4', ''),
                        survey_trend_report.get('sheet3', ''),
                        survey_trend_report.get('sheet5', ''),
                    ]
                    yoy_report = "\n".join([
                        yoy_report,
                        "\n---\n## 2026新增HC预测与热点岗位前瞻（Sheet 4）\n",
                        survey_parts[0],
                        "\n---\n## TA招聘实践趋势分析（Sheet 3）\n",
                        survey_parts[1],
                        "\n---\n## 高管任期变化趋势分析（Sheet 5）\n",
                        survey_parts[2],
                    ])
                yoy_table = comparator.export_comparison_table()
                same_company_table = comparator.export_same_company_table()

                progress.progress(1.0, text="✅ 年度对比分析完成!")

                st.session_state['yoy_curr_agg'] = curr_agg
                st.session_state['yoy_prev_agg'] = prev_agg
                st.session_state['yoy_curr_report'] = curr_report
                st.session_state['yoy_report'] = yoy_report
                st.session_state['survey_trend_report'] = survey_trend_report
                st.session_state['yoy_table'] = yoy_table
                st.session_state['same_company_table'] = same_company_table
                st.session_state['yoy_comparator'] = comparator
                st.session_state['yoy_curr_year'] = curr_year
                st.session_state['yoy_prev_year'] = prev_year
                st.session_state['yoy_mode'] = 'questionnaire'
                all_errs = curr_errs + prev_errs
                if all_errs:
                    st.session_state['yoy_errors'] = all_errs

    # 显示年度对比结果（两种模式通用）
    if 'yoy_comparator' in st.session_state:
        comparator = st.session_state['yoy_comparator']
        curr_agg = st.session_state['yoy_curr_agg']
        curr_report = st.session_state['yoy_curr_report']
        yoy_report = st.session_state['yoy_report']
        yoy_table = st.session_state['yoy_table']
        same_company_table = st.session_state.get('same_company_table', pd.DataFrame())
        cy = st.session_state['yoy_curr_year']
        py = st.session_state['yoy_prev_year']
        is_report_mode = st.session_state.get('yoy_mode') == 'report'
        same_company_available = (
            not is_report_mode
            and isinstance(same_company_table, pd.DataFrame)
            and not same_company_table.empty
        )

        if 'yoy_errors' in st.session_state:
            with st.expander("⚠️ 处理警告", expanded=False):
                for e in st.session_state['yoy_errors']:
                    st.warning(e)

        st.markdown("---")

        # 数据来源标识
        if is_report_mode:
            prev_data = st.session_state.get('yoy_prev_data')
            st.info(f"📊 **最终口径表模式**: {cy}年调研数据 vs {py}年最终口径表 ({prev_data.source_file if prev_data else 'N/A'})")
        else:
            st.info(f"📋 **问卷模式**: {cy}年调研数据 vs {py}年调研数据")

        # 概览卡片
        curr_s = curr_agg.get_summary()
        c1, c2, c3, c4 = st.columns(4)
        with c1: card(f"{cy} 公司数", f"{curr_s['公司数']}家")
        with c2:
            if is_report_mode:
                prev_data = st.session_state.get('yoy_prev_data')
                card(f"{py} 数据来源", "最终口径表")
            else:
                prev_agg = st.session_state.get('yoy_prev_agg')
                prev_s = prev_agg.get_summary() if prev_agg else {}
                card(f"{py} 公司数", f"{prev_s.get('公司数', 'N/A')}家")
        with c3:
            curr_df = curr_agg.get_dataframe()
            curr_ov = curr_df[curr_df['层级'] == '公司整体']
            card(f"{cy} 总招聘量", f"{curr_ov['招聘总量'].sum():.0f}人" if not curr_ov.empty else "N/A")
        with c4:
            if is_report_mode:
                prev_data = st.session_state.get('yoy_prev_data')
                extracted = len(prev_data.raw_extractions) if prev_data else 0
                card(f"{py} 提取指标", f"{extracted}项")
            else:
                prev_agg = st.session_state.get('yoy_prev_agg')
                if prev_agg:
                    prev_df = prev_agg.get_dataframe()
                    prev_ov = prev_df[prev_df['层级'] == '公司整体']
                    card(f"{py} 总招聘量", f"{prev_ov['招聘总量'].sum():.0f}人" if not prev_ov.empty else "N/A")
                else:
                    card(f"{py} 总招聘量", "N/A")

        # Tab页
        tabs = [
            f"📈 {py} vs {cy} 趋势对比",
            f"📊 {cy} 行业报告",
            "🔬 维度明细对比",
            "📄 完整对比报告"
        ]
        if same_company_available:
            tabs.append("🏢 同公司年度比较")
        if is_report_mode:
            tabs.append("🔍 数据提取审核")

        tab_objs = st.tabs(tabs)
        tab_yoy = tab_objs[0]
        tab_curr = tab_objs[1]
        tab_detail = tab_objs[2]
        tab_full = tab_objs[3]
        tab_same_company = tab_objs[4] if same_company_available else None
        tab_audit = tab_objs[5 if same_company_available else 4] if is_report_mode else None

        with tab_yoy:
            st.markdown(f"### {py} vs {cy} 关键指标变化一览")

            # 按模块分组显示
            for module in yoy_table['模块'].unique():
                sub = yoy_table[yoy_table['模块'] == module]
                if not sub.empty:
                    st.markdown(f"#### {module}")
                    st.dataframe(sub.drop(columns=['模块']), use_container_width=True, hide_index=True)

        with tab_curr:
            st.markdown(curr_report)

        with tab_detail:
            st.markdown("### 按维度筛选对比")
            modules = yoy_table['模块'].unique().tolist()
            sel_module = st.selectbox("选择模块", modules)
            filtered = yoy_table[yoy_table['模块'] == sel_module]
            st.dataframe(filtered.drop(columns=['模块']), use_container_width=True, hide_index=True)

            # 趋势箭头汇总
            st.markdown("### 趋势汇总")
            up_count = len(yoy_table[yoy_table['趋势'] == '↑'])
            down_count = len(yoy_table[yoy_table['趋势'] == '↓'])
            flat_count = len(yoy_table[yoy_table['趋势'] == '→'])
            na_count = len(yoy_table[yoy_table['趋势'] == ''])
            tc1, tc2, tc3, tc4 = st.columns(4)
            with tc1: card("↑ 上升", f"{up_count}项")
            with tc2: card("↓ 下降", f"{down_count}项")
            with tc3: card("→ 持平", f"{flat_count}项")
            with tc4: card("N/A", f"{na_count}项")

        with tab_full:
            st.markdown(yoy_report)

        if tab_same_company is not None:
            with tab_same_company:
                st.markdown("### 相同公司样本年度比较")
                st.caption("仅使用两年均上传且成功识别的公司，剔除只在单一年份出现的公司。")
                for module in same_company_table['模块'].unique():
                    sub = same_company_table[same_company_table['模块'] == module]
                    if not sub.empty:
                        st.markdown(f"#### {module}")
                        st.dataframe(sub.drop(columns=['模块']), use_container_width=True, hide_index=True)

        if tab_audit is not None:
            with tab_audit:
                st.markdown("### 🔍 上年度报告数据提取审核")
                st.markdown("以下是从上年度最终口径表中自动提取的数据点，请核实准确性：")
                prev_data = st.session_state.get('yoy_prev_data')
                if prev_data:
                    st.markdown(f"**报告来源**: {prev_data.source_file}")
                    st.markdown(f"**提取年份**: {prev_data.year}")
                    st.markdown(f"**提取总数**: {len(prev_data.raw_extractions)} 项")

                    st.markdown("---")
                    st.markdown("#### 提取数据摘要")
                    st.markdown(prev_data.to_summary())

                    st.markdown("---")
                    st.markdown("#### 逐项提取明细")
                    audit_rows = [{'序号': i+1, '提取内容': e}
                                  for i, e in enumerate(prev_data.raw_extractions)]
                    st.dataframe(pd.DataFrame(audit_rows), use_container_width=True, hide_index=True)

                    st.markdown("---")
                    st.info("✅ 当前使用最终口径Excel作为上年度数据源，不再依赖PDF/PPTX或中间总表抽取。")

        # 下载区
        st.markdown("---")
        st.markdown("### 📥 下载结果")
        download_cols = st.columns(4 if same_company_available else 3)
        c1, c2, c3 = download_cols[:3]
        with c1:
            mode_label = "报告模式" if is_report_mode else "问卷模式"
            st.download_button(f"📈 下载年度对比报告", data=yoy_report,
                               file_name=f"TA效能_{py}_vs_{cy}_年度对比报告_{mode_label}.md",
                               mime="text/markdown", use_container_width=True)
        with c2:
            st.download_button(f"📊 下载{cy}行业报告", data=curr_report,
                               file_name=f"行业TA效能分析报告_{cy}.md",
                               mime="text/markdown", use_container_width=True)
        with c3:
            yoy_csv = yoy_table.to_csv(index=False, encoding='utf-8-sig')
            st.download_button("📊 下载对比数据 (CSV)", data=yoy_csv,
                               file_name=f"年度对比数据_{py}_vs_{cy}.csv",
                               mime="text/csv", use_container_width=True)
        if same_company_available:
            with download_cols[3]:
                same_csv = same_company_table.to_csv(index=False, encoding='utf-8-sig')
                st.download_button("🏢 下载同公司对比 (CSV)", data=same_csv,
                                   file_name=f"同公司年度对比_{py}_vs_{cy}.csv",
                                   mime="text/csv", use_container_width=True)

    elif not can_run:
        st.markdown("---")
        if use_report_mode:
            c1, c2, c3 = st.columns(3)
            with c1: st.markdown("### 📂 Step 1\n左侧上传当年调研问卷")
            with c2: st.markdown("### 📊 Step 2\n右侧上传上年度最终口径表(XLSX)")
            with c3: st.markdown("### 📈 Step 3\n比对调研数据 vs 最终口径表")
            st.info("👆 请上传当年调研问卷和上年度最终口径表")
        else:
            c1, c2, c3 = st.columns(3)
            with c1: st.markdown("### 📂 Step 1\n分别上传两年的问卷数据")
            with c2: st.markdown("### 🤖 Step 2\nAI自动比对全维度指标")
            with c3: st.markdown("### 📈 Step 3\n生成趋势报告，量化变化")
            st.info("👆 请在上方分别上传本年度和上年度的调研问卷文件")

elif "行业" in mode:
    # ==================== 多公司行业报告模式 ====================
    st.markdown("### 上传多家公司调研问卷，生成行业级TA效能分析报告")

    uploaded_files = st.file_uploader(
        "📁 批量上传调研问卷 (Excel格式，支持多选)",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        help="选择多个公司的调研问卷文件，系统将自动识别公司名称和规模"
    )

    if uploaded_files:
        st.info(f"已选择 **{len(uploaded_files)}** 个文件")

        if st.button("🚀 开始批量分析", type="primary", use_container_width=True):
            agg = IndustryAggregator()
            progress = st.progress(0, text="准备中...")
            errors = []
            raw_list = []

            # Phase 1: 摄入到 Raw Data 池
            tmp_paths = []
            for i, f in enumerate(uploaded_files):
                progress.progress((i + 1) / (len(uploaded_files) + 4),
                                  text=f"📥 Raw Data 摄入 ({i+1}/{len(uploaded_files)}): {f.name}")
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                        tmp.write(f.getvalue())
                        tmp_path = tmp.name
                    tmp_paths.append(tmp_path)
                    raw = ingest_company(tmp_path)
                    name = agg.add_company_raw(raw)
                    raw_list.append(raw)
                except Exception as e:
                    errors.append(f"{f.name}: {str(e)}")
            # Cleanup temp files after all ingestion
            for tp in tmp_paths:
                try: os.unlink(tp)
                except: pass

            # Phase 2: 数据验证清洗
            progress.progress(0.7, text="🔍 数据验证与清洗...")
            cleaned, val_results, val_report = validate_and_clean(raw_list)

            # Phase 3: 验证通过的数据进入 Wiki
            progress.progress(0.8, text="📚 构建 Wiki 知识库...")
            for data in cleaned:
                agg.add_company(data)

            # Phase 4: 生成行业报告
            progress.progress(0.9, text="📊 生成行业报告...")
            gen = IndustryReportGenerator(agg)
            report = gen.generate_full_report()
            progress.progress(1.0, text="✅ 分析完成!")

            st.session_state['agg'] = agg
            st.session_state['industry_report'] = report
            st.session_state['gen'] = gen
            st.session_state['val_results'] = val_results
            st.session_state['val_report'] = val_report
            st.session_state['raw_count'] = len(raw_list)
            st.session_state['clean_count'] = len(cleaned)
            if errors:
                st.session_state['errors'] = errors

        # 显示结果
        if 'agg' in st.session_state:
            agg = st.session_state['agg']
            report = st.session_state['industry_report']
            gen = st.session_state['gen']
            summary = agg.get_summary()

            if 'errors' in st.session_state and st.session_state['errors']:
                with st.expander("⚠️ 处理警告", expanded=False):
                    for e in st.session_state['errors']:
                        st.warning(e)

            st.markdown("---")

            # 概览卡片
            c1, c2, c3, c4 = st.columns(4)
            with c1: card("参调公司", f"{summary['公司数']}家")
            with c2: card("A类公司", f"{len(summary['A类公司'])}家")
            with c3: card("B类公司", f"{len(summary['B类公司'])}家")
            with c4: card("数据记录", f"{summary['总记录数']}条")

            # 公司列表
            with st.expander("📋 参调公司列表", expanded=False):
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**A类公司 (≥1500人)**")
                    for n in summary['A类公司']:
                        st.markdown(f"- {n}")
                with col_b:
                    st.markdown("**B类公司 (<1500人)**")
                    for n in summary['B类公司']:
                        st.markdown(f"- {n}")

            # 审核报告
            audit_report = getattr(agg, 'audit_report', '✅ 数据审核通过')
            audit_count = len(getattr(agg, 'audit_log', []))
            if audit_count > 0:
                st.warning(f"⚠️ 数据审核发现 **{audit_count}** 条脱靶数据已自动修正（负数、超范围值等）")

            # Tab页
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "📊 行业概览", "📈 效能指标", "🔬 细分数据", "🔍 数据审核", "📋 附录", "📄 完整报告"
            ])

            df = agg.get_dataframe()
            overall = df[df['层级'] == '公司整体']
            func_df = df[(df['层级'] == '一级职能') & (df['职级'] == '整体')]

            with tab1:
                if not overall.empty:
                    c1, c2, c3 = st.columns(3)
                    with c1: card("行业总招聘量", f"{overall['招聘总量'].sum():.0f}人")
                    with c2: card("平均招聘量", f"{overall['招聘总量'].mean():.0f}人")
                    with c3: card("招聘量P50", f"{overall['招聘总量'].median():.0f}人")

                    st.markdown("### 各职能招聘量占比 P50")
                    func_df = df[(df['层级'] == '一级职能') & (df['职级'] == '整体')]
                    func_table = []
                    for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                        fd = func_df[func_df['职能'] == func]
                        ratios = []
                        for _, row in fd.iterrows():
                            ct = overall[overall['公司'] == row['公司']]['招聘总量'].values
                            if len(ct) > 0 and ct[0] > 0:
                                ratios.append(row['招聘总量'] / ct[0])
                        if ratios:
                            func_table.append({'职能': func, 'P50': f"{np.median(ratios):.2%}",
                                               '样本数': len(ratios)})
                    if func_table:
                        st.dataframe(pd.DataFrame(func_table), use_container_width=True, hide_index=True)

            with tab2:
                # 招聘渠道
                st.markdown("### 一级招聘渠道分布 P50")
                st.caption("一级渠道：HR直招、外部渠道、内部渠道；外部渠道=猎头+内推+主动投递+校招+RPO，内部渠道=内部转岗。渠道占比0值为有效值，不做trim。")
                ch_data = []
                for _, row in overall.iterrows():
                    hr_n = row.get('HR直招', 0) or 0
                    hh_n = row.get('猎头_人', 0) or 0
                    ref_n = row.get('内推_人', 0) or 0
                    rpo_n = row.get('RPO_人', 0) or 0
                    apply_n = row.get('主动投递', 0) or 0
                    campus_n = row.get('校招', 0) or 0
                    transfer_n = row.get('内部转岗', 0) or 0
                    ext_n = hh_n + ref_n + rpo_n + apply_n + campus_n
                    t = hr_n + ext_n + transfer_n
                    if t <= 0:
                        continue
                    ch_data.append({
                        '规模': row['规模'],
                        'HR直招': hr_n / t,
                        '外部渠道': ext_n / t,
                        '内部渠道': transfer_n / t,
                        '猎头': hh_n / ext_n if ext_n > 0 else np.nan,
                        '内推': ref_n / ext_n if ext_n > 0 else np.nan,
                        '主动投递': apply_n / ext_n if ext_n > 0 else np.nan,
                        '校招': campus_n / ext_n if ext_n > 0 else np.nan,
                        'RPO': rpo_n / ext_n if ext_n > 0 else np.nan,
                    })
                if ch_data:
                    chdf = pd.DataFrame(ch_data)
                    ch_table = []
                    for ch in ['HR直招', '外部渠道', '内部渠道']:
                        ch_table.append({
                            '渠道': ch,
                            '整体P50': f"{chdf[ch].median():.1%}",
                            'A类P50': f"{chdf[chdf['规模']=='A'][ch].median():.1%}" if len(chdf[chdf['规模']=='A']) > 0 else 'N/A',
                            'B类P50': f"{chdf[chdf['规模']=='B'][ch].median():.1%}" if len(chdf[chdf['规模']=='B']) > 0 else 'N/A',
                        })
                    st.dataframe(pd.DataFrame(ch_table), use_container_width=True, hide_index=True)

                    st.markdown("### 外部渠道细分 P50（占外部渠道）")
                    ext_table = []
                    for ch in ['猎头', '内推', '主动投递', '校招']:
                        ext_table.append({
                            '外部渠道': ch,
                            '整体P50': f"{chdf[ch].median():.1%}",
                            'A类P50': f"{chdf[chdf['规模']=='A'][ch].median():.1%}" if len(chdf[chdf['规模']=='A']) > 0 else 'N/A',
                            'B类P50': f"{chdf[chdf['规模']=='B'][ch].median():.1%}" if len(chdf[chdf['规模']=='B']) > 0 else 'N/A',
                        })
                    ext_table.append({
                        '外部渠道': 'RPO',
                        '整体P50': f"{chdf['RPO'].median():.1%}",
                        'A类P50': 'N/A',
                        'B类P50': 'N/A',
                    })
                    st.dataframe(pd.DataFrame(ext_table), use_container_width=True, hide_index=True)

                # 招聘周期
                st.markdown("### 各职能招聘周期 P50（天）")
                tth_table = []
                for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                    fd = func_df[func_df['职能'] == func]
                    tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
                    if len(tth) > 0:
                        tth_table.append({'职能': func, 'P50': f"{tth.median():.1f}",
                                          '样本数': len(tth)})
                if tth_table:
                    st.dataframe(pd.DataFrame(tth_table), use_container_width=True, hide_index=True)

                # 招聘成本
                st.markdown("### 渠道单个职位招聘成本 P50（万元）")
                st.caption("口径：各职能外部渠道费用成本 / 各职能（猎头+内部推荐+RPO+主动投递+校招）的招聘总数")
                cost_table = []
                for func in ['早期研发', '临床开发', '商业', '生产及供应链', '职能']:
                    fd = func_df[func_df['职能'] == func]
                    costs = []
                    for _, row in fd.iterrows():
                        c = pd.to_numeric(row.get('外部渠道成本_万'), errors='coerce')
                        external_hires = _external_cost_hire_count(row)
                        if pd.notna(c) and c > 0 and external_hires > 0:
                            costs.append(c / external_hires)
                    if costs:
                        cost_table.append({'职能': func, 'P50': f"{np.median(costs):.2f}",
                                           '样本数': len(costs)})
                if cost_table:
                    st.dataframe(pd.DataFrame(cost_table), use_container_width=True, hide_index=True)

            with tab3:
                col_rd, col_comm = st.columns(2)
                with col_rd:
                    st.markdown("### 🔬 研发细分")
                    rd = df[(df['层级'] == '研发细分') & (df['职级'] == '整体')]
                    if not rd.empty:
                        rd_table = []
                        for f in sorted(rd['职能'].unique()):
                            fd = rd[rd['职能'] == f]
                            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
                            rd_table.append({'职能': f, '样本数': len(fd),
                                             '周期P50': f"{tth.median():.1f}" if len(tth) > 0 else 'N/A'})
                        st.dataframe(pd.DataFrame(rd_table), use_container_width=True, hide_index=True)
                    else:
                        st.info("暂无研发细分数据")

                with col_comm:
                    st.markdown("### 🏪 商业细分")
                    comm = df[(df['层级'] == '商业细分') & (df['职级'] == '整体')]
                    if not comm.empty:
                        comm_table = []
                        for f in sorted(comm['职能'].unique()):
                            fd = comm[comm['职能'] == f]
                            tth = pd.to_numeric(fd['招聘周期_天'], errors='coerce').dropna()
                            comm_table.append({'职能': f, '样本数': len(fd),
                                               '周期P50': f"{tth.median():.1f}" if len(tth) > 0 else 'N/A'})
                        st.dataframe(pd.DataFrame(comm_table), use_container_width=True, hide_index=True)
                    else:
                        st.info("暂无商业细分数据")

            with tab4:
                st.markdown("### 🔍 数据清洗与验证报告")
                st.markdown("**数据流程**: 📥 Raw Data → 🔍 验证清洗 → 📚 Wiki知识库 → 📊 报告")

                # 数据流概览
                raw_count = st.session_state.get('raw_count', 0)
                clean_count = st.session_state.get('clean_count', 0)
                reject_count = raw_count - clean_count
                c1, c2, c3 = st.columns(3)
                with c1: card("📥 Raw Data", f"{raw_count}家")
                with c2: card("✅ 进入Wiki", f"{clean_count}家")
                with c3: card("❌ 待修正", f"{reject_count}家")

                st.markdown("---")

                # 验证详情
                val_results = st.session_state.get('val_results', [])
                val_report = st.session_state.get('val_report', '')

                if val_results:
                    # 各公司验证状态一览
                    st.markdown("#### 各公司验证状态")
                    status_data = []
                    for r in val_results:
                        status_icon = {'pass': '✅', 'warning': '⚠️', 'needs_correction': '❌'}.get(r.status, '❓')
                        status_data.append({
                            '状态': status_icon,
                            '公司': r.company,
                            '结果': r.status,
                            '错误': r.error_count,
                            '警告': r.warning_count,
                            '已修正': r.fixed_count,
                        })
                    st.dataframe(pd.DataFrame(status_data), use_container_width=True, hide_index=True)

                    # 问题明细
                    all_issues = []
                    for r in val_results:
                        for i in r.issues:
                            all_issues.append(i.to_dict())
                    if all_issues:
                        st.markdown(f"#### 问题明细（共{len(all_issues)}条）")
                        issue_df = pd.DataFrame(all_issues)
                        st.dataframe(issue_df, use_container_width=True, hide_index=True)

                        # 下载验证报告
                        st.download_button("📋 下载验证报告 (MD)", data=val_report,
                                           file_name="数据验证报告.md", mime="text/markdown",
                                           use_container_width=True)
                    else:
                        st.success("✅ 所有数据验证通过，无问题")

                # 后续Trim审核
                if audit_count > 0:
                    st.markdown("---")
                    st.markdown(f"#### 二次审核（Trim脱靶数据）: {audit_count}条")
                    st.markdown(audit_report)
                    audit_df = pd.DataFrame(getattr(agg, 'audit_log', []))
                    st.dataframe(audit_df, use_container_width=True, hide_index=True)

            with tab5:
                st.markdown("### 附录: 分位数详表")
                appendix_start = report.find("## 附录")
                if appendix_start > 0:
                    st.markdown(report[appendix_start:])
                else:
                    st.markdown(report)

            with tab6:
                st.markdown(report)

            # 下载区
            st.markdown("---")
            st.markdown("### 📥 下载结果")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.download_button("📄 下载行业报告 (MD)", data=report,
                                   file_name="行业TA效能分析报告.md", mime="text/markdown",
                                   use_container_width=True)
            with c2:
                csv = df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button("📊 下载汇总数据 (CSV)", data=csv,
                                   file_name="行业汇总数据.csv", mime="text/csv",
                                   use_container_width=True)
            with c3:
                kb_all = {}
                for name, kb in agg.knowledge_bases.items():
                    for eid, e in kb.entries.items():
                        kb_all[eid] = e.to_dict()
                kb_json = json.dumps(kb_all, ensure_ascii=False, indent=2, default=str)
                st.download_button("💾 下载知识库 (JSON)", data=kb_json,
                                   file_name="行业知识库.json", mime="application/json",
                                   use_container_width=True)

    else:
        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown("### 📁 Step 1\n批量上传20家公司问卷")
        with c2: st.markdown("### 🤖 Step 2\nAI自动解析，计算P25/P50/P75")
        with c3: st.markdown("### 📊 Step 3\n生成行业报告，对标PDF格式")
        st.info("👆 请上传调研问卷文件开始分析")

else:
    # ==================== 单公司模式 ====================
    st.markdown("### 上传单家公司调研问卷，生成TA效能分析报告")

    uploaded_file = st.file_uploader("📁 上传调研问卷 (Excel格式)", type=['xlsx', 'xls'])

    if uploaded_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name

        try:
            if st.button("🚀 开始分析", type="primary", use_container_width=True):
                with st.spinner("处理中..."):
                    raw = ingest_company(tmp_path)
                    company = raw.get('company_info', {}).get('公司名称', '未知')
                    kb = compile_to_wiki(raw)
                    output_dir = tempfile.mkdtemp()
                    report_text = generate_report(kb, company, output_dir)
                    checks = validate_data(kb, company, output_dir)

                st.session_state.update({
                    'single_raw': raw, 'single_kb': kb,
                    'single_report': report_text, 'single_checks': checks,
                    'single_company': company
                })

            if 'single_kb' in st.session_state:
                kb = st.session_state['single_kb']
                company = st.session_state['single_company']
                report_text = st.session_state['single_report']
                checks = st.session_state['single_checks']

                st.markdown(f"## 📈 {company} - 分析结果")

                overview = kb.get_entry(f'{company}.overview')
                if overview and isinstance(overview.metric_value, dict):
                    v = overview.metric_value
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: card("招聘总量", f"{v.get('招聘总量',0):.0f}人")
                    with c2: card("招聘周期", f"{v.get('招聘周期_天',0):.1f}天")
                    with c3: card("外部成本", f"{v.get('外部渠道成本_万',0):.0f}万")
                    with c4:
                        t = v.get('招聘总量', 0)
                        c = v.get('外部渠道成本_万', 0)
                        card("人均成本", f"{c/t:.2f}万" if t and t > 0 and c else "N/A")

                tab1, tab2, tab3 = st.tabs(["✅ 验证", "📄 报告", "📊 数据"])
                with tab1:
                    for s, n, r in checks:
                        st.markdown(f"{s} **{n}**: {r}")
                with tab2:
                    st.markdown(report_text)
                with tab3:
                    raw_df = st.session_state['single_raw'].get('main_efficiency')
                    if raw_df is not None:
                        st.dataframe(raw_df, use_container_width=True)

                st.download_button("📄 下载报告", data=report_text,
                                   file_name=f"{company}_分析报告.md", use_container_width=True)
        except Exception as e:
            st.error(f"错误: {e}")
        finally:
            try: os.unlink(tmp_path)
            except: pass
    else:
        st.info("👆 请上传调研问卷文件开始分析")
