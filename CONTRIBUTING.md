# Contributing / Working Agreement

## How to propose a change
1. Open a GitHub Issue using the **Feature request** template (see `.github/ISSUE_TEMPLATE/feature_request.md`).
2. Create a new chat with your assistant and paste the **Mini Context** (below), then describe the change narrowly.
3. Include only the functions/blocks you want to modify plus ~10–20 lines of surrounding context (not the whole file).
4. Add a tiny example (1–2 books) and expected effect.

## Mini Context (paste at the top of each new chat)

##--------------------------------

> Tip: keep `PROJECT_CONTEXT.md` open in another tab for quick copy/paste.

## Development conventions
- Small, single-purpose PRs.
- Keep logs concise (we already have per-bucket summaries).
- Document schema changes in `docs/SCHEMA.md`.
- Add new flows or cron behavior in `docs/WORKFLOW.md`.
- Update `CHANGELOG.md` if you keep one.

## Local test checklist
- Dry run with manual dispatch.
- Verify Notion property names exactly match the schema.
- Confirm routing summary (Main vs Alt) matches Catalog checkboxes.
- Check `.last_sync_at` behavior if incremental sync is toggled.
