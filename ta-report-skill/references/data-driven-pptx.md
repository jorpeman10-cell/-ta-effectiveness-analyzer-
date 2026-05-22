# Data-Driven PPTX Skill

Use this reference when a TA effectiveness report needs to become a
consulting-style slide deck. It is bundled from the supplied
`data-driven-pptx` skill and adapted to the TA report tool so PPT direction
stays close to the current reporting logic.

## Trigger

Use for requests such as:

- Convert the TA report markdown, CSV, or workbook analysis into a PPT.
- Build a consulting-style, McKinsey-style, or benchmark-report deck.
- Prepare a data-heavy TA effectiveness deck with charts, tables, and findings.

## Workflow

1. Read the latest TA report markdown and data tables before writing slide
   conclusions.
2. Define the deck framework, visual system, and page outline.
3. Build cover, TOC, chapter, content, and final pages.
4. Use charts and action titles driven by the report values, not fixed trend
   wording.
5. Verify all derived values, source notes, and slide layouts before delivery.

## TA Deck Framework

Use three report parts unless the user asks for a different structure.

1. Overall context: industry overview and talent-market background.
2. TA effectiveness metrics: recruitment volume, channels, cycle time, cost,
   productivity, TA configuration, same-company or A/B comparisons when the
   report includes them.
3. Practice insights: Sheet 3 recruitment practice analysis, Sheet 4 new HC and
   hot roles, Sheet 5 executive-tenure findings.

Keep current TA data rules intact:

- Use the report markdown and exported data tables as the source of deck data.
- Do not reuse historical survey conclusions for Sheet 3/4/5.
- Keep channel levels separate:
  - Level 1: HR direct, external channel, internal channel.
  - External detail: headhunter, referral, active application, campus, RPO.
- Preserve annotations where prior-year values use an average because a P50
  value is unavailable.

## Visual System

Style anchor: restrained consulting report, high information density, careful
data labels.

```yaml
colors:
  primary: "#1B3A5C"
  secondary: "#6B7280"
  accent: "#8B6914"
  background: "#F5F5F3"
  text: "#1C1C1E"
  light: "#FFFFFF"
  chart1: "#1B3A5C"
  chart2: "#2D5A8E"
  chart3: "#4A7FB5"
  chart4: "#6B7280"
  chart5: "#9CA3AF"
```

- Action title: every metric-page title states the conclusion supported by the
  data on that page.
- Titles: authoritative serif plus Chinese sans-serif where available.
- Body: readable Chinese sans-serif.
- Big numbers: 48-56 px for KPI highlights only.
- Footnotes: include unified source notes on all data pages.

## Layout Patterns

- Cover: strong title and subtitle with a relevant industry visual.
- Chapter page: full primary-color band or page with large part label.
- Content layout A: chart left, interpretation right.
- Content layout B: full-width table, bottom findings.
- Content layout C: KPI numbers top, supporting chart or explanation below.
- Final page: key findings and closing message.

For TA effectiveness decks, recruitment volume deserves its own page when the
report includes the function split. Do not bury it inside a mixed KPI page.

## Data Presentation Rules

### Charts

- Prefer grouped bar charts for year-over-year comparisons.
- Use lighter series for the prior year and darker series for the current year.
- Show data labels and units.
- Use tables when the sample size or source note is the finding.

### Tables

- Use dark headers, readable alternating rows, and highlighted cells only for
  decision-driving values.
- Keep P25/P50/P75 or sample annotations visible when the report depends on
  them.

### Conclusions

- Recalculate percent and percentage-point changes before writing a title.
- Do not hardcode trend direction for channel mix, cost, or productivity.
- If P50 direction is sensitive to changing samples, state the sample basis
  rather than implying a population shift.

## Update Checks

When the input report changes:

1. List changed report values and affected pages.
2. Update charts, tables, headline numbers, and action titles together.
3. Recheck channel definitions, source notes, Sheet 3/4/5 survey wording, and
   same-company annotations.
4. Run the deck checker or export validation available in the PPT workflow.

## Pitfalls

- Do not mix outside web data into the TA metric pages without explicit user
  approval.
- Do not present a prior-year survey reference as a current-year calculation.
- Do not collapse external-channel detail into internal-channel analysis.
- Do not treat incomplete TA configuration history as a full YoY comparison.
- Do not leave a slide title saying a channel increased when the chart values
  show a decline.

## PPTD Compatibility

If the PPT environment supports PPTD or `pptx-swarm`, create:

```text
<output-dir>/
  design.md
  outline.md
  <deck-name>.pptd
  pages/
    p01_cover.page
    p02_toc.page
    p03_chapter.page
```

Run the available checker after the page files are generated and resolve layout
errors before delivery.
