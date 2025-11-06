# Workflow & Scheduling

## GitHub Action
- File: `.github/workflows/readwise_sync.yml`
- Triggers: cron + manual dispatch.
- Steps: checkout → setup Python → install → run `python main.py` → commit exports.

## Incremental Sync
- Write a timestamp to `exports/.last_sync_at` at the end of each run.
- Read this file next run and pass it as `RW_UPDATED_AFTER`.
- Fallback to FULL SYNC if the file is missing.

### Code toggle (main.py)
- Replace the current `updated_after = None` with:
  ```python
  updated_after = read_last_sync_iso()

