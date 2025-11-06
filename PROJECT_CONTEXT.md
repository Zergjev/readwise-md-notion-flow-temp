# Project Context — Readwise → Notion Sync

**Purpose**  
Sync Readwise highlights (and notes) into Notion databases with routing via a Kindle Book Catalog.

## Environment Variables
- `READWISE_TOKEN`, `NOTION_TOKEN`, `NOTION_DATABASE_ID`
- `MAIN_DB_ID` (defaults to `NOTION_DATABASE_ID`)
- `ALT_DB_ID` (optional)
- `CATALOG_DB_ID` (Kindle Book Catalog DB)
- `NOTION_VERSION` (default `2022-06-28`)
- `RW_UPDATED_AFTER` (optional ISO cutoff; typically `.last_sync_at`)

## Notion DB schema (Main/Alt targets)
- **Cover** (title)
- **Highlight** (rich_text)
- **Note** (rich_text)
- **Category** (select)
- **Location** (number)
- **URL** (url)
- **Tags** (multi_select)
- **Author** (rich_text)
- **HighlightId** (rich_text, upsert key)

## Catalog DB schema
- **Cover** (title)
- **SourceId** (rich_text)
- **SendToMain** (checkbox)
- **SendToAlt** (checkbox)
- **SendToArchive** (checkbox)
- **LastSynced** (date)

## Routing Rules
1) Read the **Catalog** and assign each source a `target_db`:
   - `SendToMain` → Main DB
   - `SendToAlt`  → Alt DB (if set)
   - `SendToArchive` → skip
2) Flatten highlights. Each row carries `target_db`.
3) Upsert by **HighlightId** into the bucketed DB.
4) Main DB excludes anything routed to Alt.

## Current Focus / Open Items
- Incremental sync using `.last_sync_at`
- (Optional) hard delete from Main when a book moves to Alt
- Performance: reduce Notion reads, batch writes, tune pagination

## Key Files
- `main.py` — end-to-end sync (Readwise → routing → Notion upsert)
- `.github/workflows/readwise_sync.yml` — scheduled/manual runs

> **Paste this entire block at the top of any new chat** so assistants have the context without reading old threads.
