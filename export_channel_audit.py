from __future__ import annotations

from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generate_latest_report_markdown import DATA_ROOT  # noqa: E402
from multi_company import ingest_company  # noqa: E402


OUT_DIR = ROOT / "output_reports"


ALIASES = {
    "abv": "abbvie",
    "abbvie": "abbvie",
    "艾伯维": "abbvie",
    "abbott": "abbott",
    "雅培": "abbott",
    "beigene": "beone",
    "beone": "beone",
    "百济": "beone",
    "百济神州": "beone",
    "bms": "bms",
    "百时美施贵宝": "bms",
    "ge": "ge",
    "gilead": "gilead",
    "吉利德": "gilead",
    "msd": "msd",
    "默沙东": "msd",
    "merck": "merck",
    "默克": "merck",
    "mpcn": "mpcn",
    "丸红": "mpcn",
    "novartis": "novartis",
    "诺华": "novartis",
    "pfizer": "pfizer",
    "辉瑞": "pfizer",
    "santen": "santen",
    "参天": "santen",
    "sanofi": "sanofi",
    "赛诺菲": "sanofi",
    "sa": "sanofi",
    "viatris": "viatris",
    "晖致": "viatris",
    "eisai": "eisai",
    "卫材": "eisai",
    "organon": "organon",
    "欧加隆": "organon",
    "欧加农": "organon",
    "kenvue": "kenvue",
    "科赴": "kenvue",
    "dizal": "dizal",
    "迪哲": "dizal",
    "roche": "roche",
    "罗氏": "roche",
    "信达": "xinda",
    "信达生物": "xinda",
    "smpc": "smpc",
    "住友": "smpc",
}


def safe(v) -> float:
    try:
        x = float(v)
        return x if pd.notna(x) else 0.0
    except Exception:
        return 0.0


def normalize_company(name: str, file_name: str = "") -> str:
    text = f"{name or ''} {file_name or ''}".strip().lower()
    text = re.sub(r"20\d{2}", "", text)
    text = re.sub(
        r"ta|效能|问卷|调研|数据|中国|制药|医药|有限公司|股份|集团|投资|上海|北京|\s+",
        "",
        text,
    )
    text = re.sub(r"[_\-.（）()【】\[\] ]+", "", text)

    # Order matters: avoid matching Santen as Sanofi/SA, and MSD as Merck.
    for key in ["santen", "参天", "sanofi", "赛诺菲", "msd", "默沙东", "merck", "默克"]:
        if key in text:
            return ALIASES[key]
    for key, value in ALIASES.items():
        if key in text:
            return value
    return text or ""


def clean_paths(paths):
    return [
        p
        for p in sorted(paths)
        if "__MACOSX" not in str(p)
        and not p.name.startswith(("._", "~$"))
        and p.suffix.lower() in {".xlsx", ".xls"}
    ]


def get_year_paths():
    base = DATA_ROOT.parents[1]
    root_2024 = sorted([p for p in base.iterdir() if p.is_dir() and "2024" in p.name])[0]
    return {
        2024: clean_paths(root_2024.rglob("*.xlsx")),
        2025: clean_paths(DATA_ROOT.glob("*.xlsx")),
    }


def collect_channel_rows() -> pd.DataFrame:
    rows = []
    for year, paths in get_year_paths().items():
        for path in paths:
            try:
                raw = ingest_company(str(path))
                info = raw.get("company_info", {}) or {}
                company = info.get("公司名称", "")
                scale = info.get("公司规模分类", "")
                key = normalize_company(company, path.name)
                main = raw.get("main_efficiency")
                if main is None or main.empty or "职能" not in main.columns:
                    rows.append(
                        {
                            "year": year,
                            "file": path.name,
                            "company": company,
                            "key": key,
                            "scale": scale,
                            "status": "NO_MAIN",
                        }
                    )
                    continue

                overall = main[main["职能"].astype(str).str.contains("公司整体", na=False)]
                if overall.empty:
                    rows.append(
                        {
                            "year": year,
                            "file": path.name,
                            "company": company,
                            "key": key,
                            "scale": scale,
                            "status": "NO_COMPANY_OVERALL",
                        }
                    )
                    continue

                r = overall.iloc[0]
                total = safe(r.get("招聘总量"))
                hr = safe(r.get("HR直招"))
                hh = safe(r.get("猎头_人"))
                rpo = safe(r.get("RPO_人"))
                referral = safe(r.get("内推_人"))
                active = safe(r.get("主动投递"))
                campus = safe(r.get("校招"))
                transfer = safe(r.get("内部转岗"))
                third_party = safe(r.get("三方员工招聘总量"))
                ext = hh + rpo + referral + active + campus
                channel_total = hr + ext + transfer

                adjusted_total = total
                adjustment = ""
                if (
                    total > 0
                    and channel_total > 0
                    and third_party > 0
                    and abs((total - channel_total) - third_party) < 1e-6
                ):
                    adjusted_total = channel_total
                    adjustment = "招聘总量原值包含三方员工招聘，已按渠道闭合口径扣除三方员工"

                rows.append(
                    {
                        "year": year,
                        "file": path.name,
                        "company": company,
                        "key": key,
                        "scale": scale,
                        "status": "OK" if adjusted_total > 0 and channel_total > 0 else "NO_VALID_CHANNEL_TOTAL",
                        "招聘总量_原始": total if total > 0 else np.nan,
                        "招聘总量_渠道闭合口径": adjusted_total if adjusted_total > 0 else np.nan,
                        "HR直招人数": hr,
                        "猎头人数": hh,
                        "RPO人数": rpo,
                        "内推人数": referral,
                        "主动投递人数": active,
                        "校招人数": campus,
                        "内部转岗人数": transfer,
                        "外部渠道人数": ext,
                        "一级渠道合计": channel_total if channel_total > 0 else np.nan,
                        "三方员工招聘总量": third_party,
                        "招聘总量原始与一级渠道差额": total - channel_total
                        if total > 0 and channel_total > 0
                        else np.nan,
                        "招聘总量调整说明": adjustment,
                        "HR直招占比": hr / channel_total if channel_total > 0 else np.nan,
                        "外部渠道占比": ext / channel_total if channel_total > 0 else np.nan,
                        "内部渠道占比": transfer / channel_total if channel_total > 0 else np.nan,
                        "猎头占外部": hh / ext if ext > 0 else np.nan,
                        "内推占外部": referral / ext if ext > 0 else np.nan,
                        "主动投递占外部": active / ext if ext > 0 else np.nan,
                        "校招占外部": campus / ext if ext > 0 else np.nan,
                        "RPO占外部": rpo / ext if ext > 0 else np.nan,
                    }
                )
            except Exception as exc:
                rows.append({"year": year, "file": path.name, "status": f"INGEST_ERROR: {exc}"})
    return pd.DataFrame(rows)


def build_quantiles(raw: pd.DataFrame) -> pd.DataFrame:
    raw_ok = raw[raw["status"].eq("OK")].copy()
    common_keys = sorted(
        (set(raw_ok.loc[raw_ok["year"].eq(2024), "key"]) & set(raw_ok.loc[raw_ok["year"].eq(2025), "key"]))
        - {"msd"}
    )
    raw["同公司匹配样本"] = raw["key"].isin(common_keys) & raw["status"].eq("OK")
    matched = raw[raw["同公司匹配样本"]].copy()

    metrics = {
        "HR直招占比": "一级渠道-HR直招",
        "外部渠道占比": "一级渠道-外部渠道",
        "内部渠道占比": "一级渠道-内部渠道",
        "猎头占外部": "外部细分-猎头",
        "内推占外部": "外部细分-内推",
        "主动投递占外部": "外部细分-主动投递",
        "校招占外部": "外部细分-校招",
        "RPO占外部": "外部细分-RPO",
    }
    rows = []
    for subset_name, df in [("全量有效样本", raw_ok), ("同公司匹配样本", matched)]:
        for year in [2024, 2025]:
            sub = df[df["year"].eq(year)]
            for metric, label in metrics.items():
                s = sub[metric].dropna()
                rows.append(
                    {
                        "样本口径": subset_name,
                        "year": year,
                        "指标": label,
                        "有效公司数": int(s.shape[0]),
                        "P25": s.quantile(0.25) if not s.empty else np.nan,
                        "P50": s.quantile(0.50) if not s.empty else np.nan,
                        "P75": s.quantile(0.75) if not s.empty else np.nan,
                        "平均值": s.mean() if not s.empty else np.nan,
                    }
                )
    quant = pd.DataFrame(rows)
    for col in ["P25", "P50", "P75", "平均值"]:
        quant[col] = quant[col].map(lambda x: round(x * 100, 2) if pd.notna(x) else np.nan)
    return quant


def write_coverage(raw: pd.DataFrame, quant: pd.DataFrame) -> Path:
    raw_ok = raw[raw["status"].eq("OK")].copy()
    common_keys = sorted(set(raw.loc[raw["同公司匹配样本"], "key"]))
    only_2024 = sorted(set(raw_ok.loc[raw_ok["year"].eq(2024), "key"]) - set(raw_ok.loc[raw_ok["year"].eq(2025), "key"]))
    only_2025 = sorted(set(raw_ok.loc[raw_ok["year"].eq(2025), "key"]) - set(raw_ok.loc[raw_ok["year"].eq(2024), "key"]))
    invalid = raw[~raw["status"].eq("OK")][["year", "file", "company", "key", "status"]]

    def markdown_table(df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        cols = list(df.columns)
        lines = [
            "| " + " | ".join(str(c) for c in cols) + " |",
            "| " + " | ".join(["---"] * len(cols)) + " |",
        ]
        for _, row in df.iterrows():
            vals = []
            for col in cols:
                val = row[col]
                vals.append("" if pd.isna(val) else str(val))
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)

    md = [
        "# 2024/2025 渠道数据批次与分位数核查",
        "",
        "## 样本结论",
        "",
        f"- 全量有效样本：2024年 {raw_ok[raw_ok['year'].eq(2024)].shape[0]} 家，"
        f"2025年 {raw_ok[raw_ok['year'].eq(2025)].shape[0]} 家。",
        f"- 同公司匹配样本：{len(common_keys)} 家，key = {', '.join(common_keys)}。",
        f"- 仅2024有效但2025未进入同公司样本：{', '.join(only_2024) if only_2024 else '无'}。",
        f"- 仅2025有效但2024未进入同公司样本：{', '.join(only_2025) if only_2025 else '无'}。",
        "- 因此，“全量整体P50”不是同一批公司；若要观察同一批公司变化，应看“同公司匹配样本”。",
        "",
        "## 分位数表说明",
        "",
        "- 一级渠道分母：HR直招 + 外部渠道 + 内部渠道。",
        "- 外部渠道：猎头 + 内推 + 主动投递 + 校招 + RPO。",
        "- 外部细分分母：外部渠道人数。",
        "- 输出文件：`channel_company_raw_2024_2025.csv`、`channel_quantiles_2024_2025.csv`。",
        "",
        "## 一级渠道分位数预览",
        "",
        markdown_table(quant[quant["指标"].str.startswith("一级渠道")]),
        "",
        "## 非有效/未纳入样本",
        "",
        markdown_table(invalid) if not invalid.empty else "无。",
    ]
    path = OUT_DIR / "channel_sample_coverage_2024_2025.md"
    path.write_text("\n".join(md), encoding="utf-8")
    return path


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    raw = collect_channel_rows()
    quant = build_quantiles(raw)

    raw_out = OUT_DIR / "channel_company_raw_2024_2025.csv"
    quant_out = OUT_DIR / "channel_quantiles_2024_2025.csv"
    raw.to_csv(raw_out, index=False, encoding="utf-8-sig")
    quant.to_csv(quant_out, index=False, encoding="utf-8-sig")
    md_out = write_coverage(raw, quant)

    raw_ok = raw[raw["status"].eq("OK")]
    print(f"RAW={raw_out}")
    print(f"QUANT={quant_out}")
    print(f"MD={md_out}")
    print(f"ALL_VALID_COUNTS={raw_ok.groupby('year')['key'].nunique().to_dict()}")
    print(f"MATCHED_KEYS={', '.join(sorted(set(raw.loc[raw['同公司匹配样本'], 'key'])))}")
    print(quant[quant["指标"].str.startswith("一级渠道")].to_string(index=False))


if __name__ == "__main__":
    main()
