---
name: ta-report-analyzer
description: Use the TA report analyzer for medical-health TA questionnaire ingestion, yearly comparison, report markdown generation, Sheet 3/4/5 survey analysis, channel/cost/productivity audits, and Lobe MCP packaging or operation.
---

# TA Report Analyzer

Use the analyzer modules already present in this repository. Keep reporting logic
aligned with `REPORT_STRUCTURE.md`, `TOOL_ADJUSTMENTS.md`, and the current
`yoy_comparator.py` implementation.

## Preferred Workflow

1. Audit questionnaire ingestion when source coverage or outliers are in doubt.
2. For the current report workflow, compare current questionnaires with the
   prior-year final metric workbook, not OCR or PDF extraction when final metrics
   are available.
3. Keep Sheet 3, Sheet 4, and Sheet 5 survey sections based on the current-year
   questionnaires only. Historical reports may guide dimensions and wording.
4. For channel analysis, keep the two levels separate:
   - Level 1: HR direct, external channel, internal channel.
   - External detail: headhunter, referral, active application, campus, RPO.
5. Preserve annotations when a reported prior-year value uses an average because
   the final table lacks a P50 value.

## Lobe MCP

Use `lobe_mcp/server.py` when the work should be callable from Lobe.

- `ta_compare_with_prior_metrics` is the default published-report comparison tool.
- `ta_compare_questionnaire_years` is for raw workbook versus raw workbook.
- `ta_generate_industry_report` is for one-year report markdown.
- `ta_extract_survey_sections` is for Sheet 3/4/5 only.
- `ta_audit_questionnaires` is for coverage and parsing checks.

Read `LOBE_MCP.md` when configuring Lobe Desktop or a hosted MCP endpoint.
