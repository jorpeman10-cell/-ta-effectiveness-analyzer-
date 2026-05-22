"""MCP server for calling TA report workflows from Lobe."""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lobe_mcp import service  # noqa: E402


mcp = FastMCP(
    name="TA Report Analyzer",
    stateless_http=True,
    json_response=True,
    host=os.getenv("HOST", "127.0.0.1"),
    port=int(os.getenv("PORT", "8000")),
)


class McpPostAcceptMiddleware:
    """Keep hosted browser clients on the JSON response path for MCP POSTs."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("method") == "POST":
            headers = list(scope.get("headers", []))
            headers = [(key, value) for key, value in headers if key != b"accept"]
            headers.append((b"accept", b"application/json, text/event-stream"))
            scope = {**scope, "headers": headers}
        await self.app(scope, receive, send)


@mcp.tool()
def ta_generate_industry_report(
    questionnaire_paths: list[str],
    include_survey_sections: bool = True,
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict:
    """Generate the current-year TA industry report from Excel questionnaires.

    Provide local Excel file paths or a local folder path. Sheet 3/4/5 survey
    sections use current-year questionnaires only.
    """
    return service.generate_industry_report(
        questionnaire_paths,
        include_survey_sections=include_survey_sections,
        curr_year=curr_year,
        prev_year=prev_year,
    )


@mcp.tool()
def ta_compare_questionnaire_years(
    current_questionnaire_paths: list[str],
    previous_questionnaire_paths: list[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict:
    """Compare two batches of TA questionnaires and return YoY markdown reports."""
    return service.compare_questionnaire_years(
        current_questionnaire_paths,
        previous_questionnaire_paths,
        curr_year=curr_year,
        prev_year=prev_year,
    )


@mcp.tool()
def ta_compare_with_prior_metrics(
    current_questionnaire_paths: list[str],
    prior_metric_paths: list[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict:
    """Compare current questionnaires with final prior-year metric workbooks.

    Use this for the current reporting workflow when the prior-year benchmark
    should come from the final published metric table rather than raw PDF text.
    """
    return service.compare_with_prior_metrics(
        current_questionnaire_paths,
        prior_metric_paths,
        curr_year=curr_year,
        prev_year=prev_year,
    )


@mcp.tool()
def ta_extract_survey_sections(
    questionnaire_paths: list[str],
    curr_year: str = "2025",
    prev_year: str = "2024",
) -> dict:
    """Extract Sheet 3, Sheet 4, and Sheet 5 survey trend sections."""
    return service.extract_survey_sections(
        questionnaire_paths,
        curr_year=curr_year,
        prev_year=prev_year,
    )


@mcp.tool()
def ta_audit_questionnaires(questionnaire_paths: list[str]) -> dict:
    """Audit TA questionnaire parsing, validation, and flattened record coverage."""
    return service.audit_questionnaires(questionnaire_paths)


@mcp.tool()
def ta_get_pptx_report_skill() -> dict:
    """Return the bundled data-driven PPT skill for TA report deck generation."""
    return service.get_pptx_report_skill()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TA Report MCP server.")
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
        help="Use stdio for Lobe Desktop or streamable-http for a hosted endpoint.",
    )
    args = parser.parse_args()
    if args.transport == "stdio":
        mcp.run(transport=args.transport)
        return

    import uvicorn

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette):
        async with mcp.session_manager.run():
            yield

    # Lobe Web is a browser-based MCP client, so expose the HTTP transport
    # through a CORS-enabled ASGI app instead of the direct FastMCP runner.
    app = Starlette(
        routes=[Mount("/", app=mcp.streamable_http_app())],
        lifespan=lifespan,
    )
    app = McpPostAcceptMiddleware(app)
    cors_app = CORSMiddleware(
        app,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )
    uvicorn.run(
        cors_app,
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
    )


if __name__ == "__main__":
    main()
