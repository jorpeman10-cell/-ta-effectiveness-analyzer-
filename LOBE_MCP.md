# TA Report Lobe MCP

This wrapper exposes the current TA report logic as MCP tools for Lobe.

## Tools

- `ta_generate_industry_report`: current-year multi-company report from survey workbooks.
- `ta_compare_with_prior_metrics`: current-year survey workbooks versus prior-year final metric table or report files.
- `ta_compare_questionnaire_years`: raw questionnaire versus raw questionnaire YoY comparison, including same-company output.
- `ta_extract_survey_sections`: Sheet 3, Sheet 4, and Sheet 5 sections only.
- `ta_audit_questionnaires`: ingestion, validation, and trim audit details.

## Current report conventions

- Current-year metrics are parsed from the uploaded questionnaire workbooks.
- Prior-year report mode should prefer the final published metric workbook.
- Sheet 3/4/5 conclusions use current-year questionnaire data only.
- Channel analysis uses the fixed logic in the main analyzer:
  - Level 1: HR direct, external channel, internal channel.
  - External channel detail: headhunter, referral, active application, campus, RPO.
  - Internal channel means internal transfer.
- TA configuration stays current-year only when prior-year coverage is incomplete.

## Local Lobe Desktop

1. Install dependencies in this project:

   ```powershell
   python -m pip install -r requirements.txt
   ```

2. In Lobe custom MCP, import or copy the stdio config from `lobe_mcp/lobe-stdio-config.json`.
3. Test the connection in Lobe and enable the tools for the agent.
4. Ask Lobe to call the tools with local workbook paths or a local folder path.

Example request:

```text
Use ta_compare_with_prior_metrics.
Current questionnaires: D:\TA\2025\questionnaires
Prior metric workbook: D:\TA\2024\final_metrics.xlsx
Return the markdown report and list any ingest errors.
```

## Lobe Web Or Cloud

Lobe Web needs a public HTTPS MCP endpoint. The existing Streamlit Community
Cloud app can stay as the report UI, but its `streamlit.app` page is not the
MCP endpoint. Deploy this MCP server as a Python web service and import its
HTTPS `/mcp` endpoint in Lobe.

Run the same server locally with Streamable HTTP:

```powershell
python lobe_mcp\server.py --transport streamable-http
```

The local endpoint is `http://127.0.0.1:8000/mcp`. Browsers may show a `406`
message for a plain GET request because MCP clients negotiate the stream
response headers.

## Render HTTPS Deployment

This repo includes `render.yaml` for a dedicated MCP web service.

1. Push the repo to GitHub.
2. In Render, create a Blueprint or Web Service from this repository.
3. If Render asks for the service root, keep the repository root for this
   project.
4. Wait for the service to deploy and copy its `onrender.com` HTTPS URL.
5. In Lobe custom MCP, choose Streamable HTTP and use:

   ```text
   https://<render-service>.onrender.com/mcp
   ```

The server reads `HOST` and `PORT` from the deployment environment. `render.yaml`
sets `HOST=0.0.0.0`, while Render provides the public service port. The HTTP
server uses MCP's stateless JSON response mode for a hosted Lobe connection.

Then configure Lobe with the HTTPS `/mcp` endpoint exposed by the hosted server.
For a remote server, file paths must be paths the server can read. If Lobe users
need browser uploads, add storage or an upload gateway before calling these tools.
