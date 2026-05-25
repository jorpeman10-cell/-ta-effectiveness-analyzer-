"""Reusable TA report workflows exposed by the Lobe MCP server."""

from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any, Iterable

from data_validator import validate_and_clean
from multi_company import IndustryAggregator, IndustryReportGenerator, ingest_company
from report_parser import PriorYearData, parse_published_reports
from survey_parser import SurveyAggregator, SurveyReportGenerator, ingest_surveys
from yoy_comparator import YoYComparator, YoYReportComparator


EXCEL_EXTENSIONS = {".xlsx", ".xls"}
REPORT_EXTENSIONS = {".xlsx", ".xls", ".pdf", ".ppt", ".pptx"}
PPTX_SKILL_REFERENCE = (
    Path(__file__).resolve().parents[1]
    / "ta-report-skill"
    / "references"
    / "data-driven-pptx.md"
)


@dataclass
class QuestionnaireBatch:
    files: list[str]
    aggregator: IndustryAggregator
    survey_aggregator: SurveyAggregator
    raw_count: int
    cleaned_count: int
    validation_report: str
    errors: list[str]


def _expand_paths(paths: Iterable[str], allowed_extensions: set[str]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    missing: list[str] = []

    for raw in paths or []:
        value = str(raw).strip().strip('"')
        if not value:
            continue

        candidates: list[Path]
        path = Path(value).expanduser()
        if path.is_dir():
            candidates = sorted(
                item for item in path.iterdir()
                if item.is_file() and item.suffix.lower() in allowed_extensions
            )
        elif path.is_file():
            candidates = [path]
        else:
            candidates = []
            for item in glob(value):
                candidate = Path(item)
                if candidate.is_file() and candidate.suffix.lower() in allowed_extensions:
                    candidates.append(candidate)
            candidates.sort()
            if not candidates:
                missing.append(value)
                continue

        for candidate in candidates:
            if candidate.name.startswith("~$"):
                continue
            if candidate.suffix.lower() not in allowed_extensions:
                continue
            resolved = str(candidate.resolve())
            if resolved not in seen:
                seen.add(resolved)
                expanded.append(resolved)

    if missing:
        raise FileNotFoundError("File path not found: " + "; ".join(missing))
    if not expanded:
        allowed = ", ".join(sorted(allowed_extensions))
        raise ValueError(f"No supported files found. Expected: {allowed}")
    return expanded


def _company_name_scale(raw: dict[str, Any], fallback_name: str) -> tuple[str, str]:
    info = raw.get("company_info", {}) if isinstance(raw, dict) else {}
    company = info.get("公司名称") or info.get("company_name") or fallback_name
    scale = info.get("公司规模分类") or info.get("公司规模") or "B"
    scale = str(scale).strip().upper()
    if scale not in {"A", "B"}:
        scale = "B"
    return str(company).strip() or fallback_name, scale


def _ingest_questionnaires(paths: Iterable[str]) -> QuestionnaireBatch:
    files = _expand_paths(paths, EXCEL_EXTENSIONS)
    raw_list: list[dict[str, Any]] = []
    errors: list[str] = []
    survey_aggregator = SurveyAggregator()

    for filepath in files:
        path = Path(filepath)
        try:
            raw = ingest_company(filepath)
            raw_list.append(raw)
            company, scale = _company_name_scale(raw, path.stem.strip())
            survey = ingest_surveys(filepath, company, scale)
            survey_aggregator.add_company(
                company,
                scale,
                sheet3=survey.get("sheet3"),
                sheet4=survey.get("sheet4"),
                sheet5=survey.get("sheet5"),
            )
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    cleaned, _, validation_report = validate_and_clean(raw_list)
    aggregator = IndustryAggregator()
    for item in cleaned:
        aggregator.add_company(item)

    return QuestionnaireBatch(
        files=files,
        aggregator=aggregator,
        survey_aggregator=survey_aggregator,
        raw_count=len(raw_list),
        cleaned_count=len(cleaned),
        validation_report=str(validation_report or ""),
        errors=errors,
    )


def _batch_meta(batch: QuestionnaireBatch) -> dict[str, Any]:
    dataframe = batch.aggregator.get_dataframe()
    return {
        "files": batch.files,
        "uploaded_file_count": len(batch.files),
        "ingested_file_count": batch.raw_count,
        "cleaned_company_count": batch.cleaned_count,
        "survey_company_count": len(batch.survey_aggregator.companies),
        "flattened_record_count": int(len(dataframe)),
        "company_names": list(batch.aggregator.companies.keys()),
        "errors": batch.errors,
        "validation_report": batch.validation_report,
    }


def generate_industry_report(
    questionnaire_paths: Iterable[str],
    include_survey_sections: bool = True,
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict[str, Any]:
    """Generate the multi-company current-year report from questionnaire workbooks."""
    batch = _ingest_questionnaires(questionnaire_paths)
    report = IndustryReportGenerator(batch.aggregator).generate_full_report()

    if include_survey_sections:
        survey_report = SurveyReportGenerator(batch.survey_aggregator).generate_split_reports(
            curr_year=curr_year,
            prev_year=prev_year,
        )
        report = "\n".join(
            [
                report,
                "\n---\n## 新增HC预测与热点岗位前瞻（Sheet 4）\n",
                survey_report["sheet4"],
                "\n---\n## TA招聘实践趋势分析（Sheet 3）\n",
                survey_report["sheet3"],
                "\n---\n## 高管任期变化趋势分析（Sheet 5）\n",
                survey_report["sheet5"],
            ]
        )

    return {"report_markdown": report, **_batch_meta(batch)}


def extract_survey_sections(
    questionnaire_paths: Iterable[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict[str, Any]:
    """Extract Sheet 3/4/5 trend sections from the current survey workbooks only."""
    batch = _ingest_questionnaires(questionnaire_paths)
    sections = SurveyReportGenerator(batch.survey_aggregator).generate_split_reports(
        curr_year=curr_year,
        prev_year=prev_year,
    )
    return {"survey_sections": sections, **_batch_meta(batch)}


def compare_questionnaire_years(
    current_questionnaire_paths: Iterable[str],
    previous_questionnaire_paths: Iterable[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict[str, Any]:
    """Compare two questionnaire batches and include same-company comparison output."""
    current = _ingest_questionnaires(current_questionnaire_paths)
    previous = _ingest_questionnaires(previous_questionnaire_paths)
    comparator = YoYComparator(
        current.aggregator,
        previous.aggregator,
        curr_year=curr_year,
        prev_year=prev_year,
    )
    report = comparator.generate_yoy_report()
    survey_sections = SurveyReportGenerator(current.survey_aggregator).generate_split_reports(
        curr_year=curr_year,
        prev_year=prev_year,
    )
    report = "\n".join(
        [
            report,
            "\n---\n## 新增HC预测与热点岗位前瞻（Sheet 4）\n",
            survey_sections["sheet4"],
            "\n---\n## TA招聘实践趋势分析（Sheet 3）\n",
            survey_sections["sheet3"],
            "\n---\n## 高管任期变化趋势分析（Sheet 5）\n",
            survey_sections["sheet5"],
        ]
    )
    return {
        "report_markdown": report,
        "comparison_table_markdown": comparator.export_comparison_table().to_markdown(index=False),
        "same_company_table_markdown": comparator.export_same_company_table().to_markdown(index=False),
        "current": _batch_meta(current),
        "previous": _batch_meta(previous),
    }


def compare_with_prior_metrics(
    current_questionnaire_paths: Iterable[str],
    prior_metric_paths: Iterable[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict[str, Any]:
    """Compare current questionnaires with final prior-year metric tables or reports."""
    current = _ingest_questionnaires(current_questionnaire_paths)
    prior_files = _expand_paths(prior_metric_paths, REPORT_EXTENSIONS)
    prior_data = parse_published_reports(prior_files, year=prev_year)
    survey_sections = SurveyReportGenerator(current.survey_aggregator).generate_split_reports(
        curr_year=curr_year,
        prev_year=prev_year,
    )
    comparator = YoYReportComparator(
        current.aggregator,
        prior_data,
        curr_year=curr_year,
        prev_year=prev_year,
        survey_trend_report=survey_sections,
    )
    return {
        "report_markdown": comparator.generate_yoy_report(),
        "comparison_table_markdown": comparator.export_comparison_table().to_markdown(index=False),
        "prior_metric_files": prior_files,
        "current": _batch_meta(current),
    }


def _plain_ta_config(values: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Convert TA configuration stats to JSON-safe Python scalars."""
    normalized: dict[str, dict[str, Any]] = {}
    for label, metrics in values.items():
        record: dict[str, Any] = {}
        for key, value in metrics.items():
            if value is None:
                record[key] = None
                continue
            try:
                record[key] = None if value != value else (int(value) if key.endswith("_n") else float(value))
            except TypeError:
                record[key] = value
        normalized[label] = record
    return normalized


def _ta_config_markdown(
    values: dict[str, dict[str, Any]],
    include_samples: bool,
) -> str:
    headers = ["配置维度", "TA FTE P50", "第三方TA/RPO P50"]
    if include_samples:
        headers = ["配置维度", "TA FTE P50", "TA FTE有效样本", "第三方TA/RPO P50", "第三方有效样本"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for label, metrics in values.items():
        fte = metrics.get("TA_FTE_P50")
        third = metrics.get("TA_第三方_P50")
        fields = [label, "N/A" if fte is None else f"{fte:.2f}"]
        if include_samples:
            fields.append(str(int(metrics.get("TA_FTE_n", 0))))
        fields.append("N/A" if third is None else f"{third:.2f}")
        if include_samples:
            fields.append(str(int(metrics.get("TA_第三方_n", 0))))
        lines.append("| " + " | ".join(fields) + " |")
    return "\n".join(lines)


def extract_ta_configuration(
    current_questionnaire_paths: Iterable[str],
    previous_questionnaire_paths: Iterable[str] | None = None,
    prior_metric_paths: Iterable[str] | None = None,
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict[str, Any]:
    """Extract internal TA FTE and external third-party/RPO personnel configuration."""
    current = _ingest_questionnaires(current_questionnaire_paths)
    prior_data = PriorYearData(year=prev_year)
    prior_files: list[str] = []
    previous_batch: QuestionnaireBatch | None = None
    if previous_questionnaire_paths:
        previous_batch = _ingest_questionnaires(previous_questionnaire_paths)
    elif prior_metric_paths:
        prior_files = _expand_paths(prior_metric_paths, REPORT_EXTENSIONS)
        prior_data = parse_published_reports(prior_files, year=prev_year)

    comparator = YoYReportComparator(
        current.aggregator,
        prior_data,
        curr_year=curr_year,
        prev_year=prev_year,
    )
    current_overall = _plain_ta_config(comparator._curr_ta_config_summary())
    current_a = _plain_ta_config(comparator._curr_ta_config_summary("A"))
    current_b = _plain_ta_config(comparator._curr_ta_config_summary("B"))
    if previous_batch:
        previous_comparator = YoYReportComparator(
            previous_batch.aggregator,
            PriorYearData(year=prev_year),
            curr_year=prev_year,
            prev_year="",
        )
        previous_overall = _plain_ta_config(previous_comparator._curr_ta_config_summary())
        previous_a = _plain_ta_config(previous_comparator._curr_ta_config_summary("A"))
        previous_b = _plain_ta_config(previous_comparator._curr_ta_config_summary("B"))
    else:
        previous_overall = _plain_ta_config(prior_data.ta_config)
        previous_a = _plain_ta_config(prior_data.ta_config_a)
        previous_b = _plain_ta_config(prior_data.ta_config_b)

    markdown_sections = [
        f"### {curr_year} TA人员配置P50（0值已trim）",
        _ta_config_markdown(current_overall, include_samples=True),
    ]
    if previous_overall:
        markdown_sections.extend(
            [
                (
                    f"\n### {prev_year} TA人员配置P50（0值已trim）"
                    if previous_batch
                    else f"\n### {prev_year} 可读取的TA人员配置值（局部口径）"
                ),
                _ta_config_markdown(previous_overall, include_samples=previous_batch is not None),
            ]
        )
        if not previous_batch:
            markdown_sections.append(
                "\n> 上年度最终口径表的TA配置页为嵌入图表，目前仅可读取 COE function 和 TA BP；不用于完整人员配置年度同比。"
            )

    return {
        "metric_definitions": {
            "TA_FTE": "内部TA人员数量（人）",
            "TA_第三方": "外部第三方招聘人员数量（包括RPO，人）",
        },
        "current_year": {
            "year": curr_year,
            "coverage": "问卷可解析的TA人员配置数据；统计口径为正值样本P50，0值trim。",
            "overall": current_overall,
            "a_class": current_a,
            "b_class": current_b,
        },
        "previous_year": {
            "year": prev_year,
            "metric_files": prior_files,
            "coverage": (
                "上年度问卷可解析的TA人员配置数据；统计口径为正值样本P50，0值trim。"
                if previous_batch
                else (
                    "仅最终口径表中嵌入图表可识别的 COE function 与 TA BP 标注值；"
                    "数据不完整，不建议与当年全量配置直接同比。"
                    if previous_overall
                    else (
                        "已提供的上年度最终口径表不包含TA人员配置页或可识别配置值，"
                        "无法从该文件提取上年度人员配置。"
                        if prior_files
                        else "未提供上年度问卷或最终口径表，未提取上年度人员配置。"
                    )
                )
            ),
            "source_mode": (
                "questionnaire"
                if previous_batch
                else ("final_metric_file" if prior_files else "not_provided")
            ),
            "questionnaire_meta": _batch_meta(previous_batch) if previous_batch else None,
            "overall": previous_overall,
            "a_class": previous_a,
            "b_class": previous_b,
        },
        "table_markdown": "\n\n".join(markdown_sections),
        "current": _batch_meta(current),
    }


def audit_questionnaires(questionnaire_paths: Iterable[str]) -> dict[str, Any]:
    """Audit ingestion, validation, and flattened record coverage for questionnaires."""
    batch = _ingest_questionnaires(questionnaire_paths)
    audit_log = batch.aggregator.run_audit()
    meta = _batch_meta(batch)
    meta.update(
        {
            "trimmed_audit_count": len(audit_log),
            "trimmed_audit_report": getattr(batch.aggregator, "audit_report", ""),
        }
    )
    return meta


def get_pptx_report_skill() -> dict[str, Any]:
    """Return the bundled TA PPT report-generation skill reference."""
    return {
        "skill_name": "data-driven-pptx",
        "scope": "TA effectiveness report decks",
        "skill_markdown": PPTX_SKILL_REFERENCE.read_text(encoding="utf-8"),
        "usage_note": (
            "Use this after TA report markdown or comparison tables are available. "
            "Keep slide conclusions tied to the latest report values."
        ),
    }
