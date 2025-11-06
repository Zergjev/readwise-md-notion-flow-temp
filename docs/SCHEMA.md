# Notion Schema

## Main/Alt target DBs (Highlights & Notes)

| Code Key      | Notion Property | Type        | Notes                           |
|---------------|------------------|-------------|---------------------------------|
| `title`       | Cover            | title       | Book title                      |
| `highlight`   | Highlight        | rich_text   | Highlight text                  |
| `note`        | Note             | rich_text   | Reader’s note                   |
| `category`    | Category         | select      | e.g., Books                     |
| `location`    | Location         | number      | Kindle location                 |
| `url`         | URL              | url         | Source or highlight URL         |
| `tags`        | Tags             | multi_select| Readwise tags                   |
| `author`      | Author           | rich_text   | Author name                     |
| `highlightid` | HighlightId      | rich_text   | **Upsert key** (string id)      |

> These names must exactly match in Notion.

## Catalog DB (Kindle Book Catalog)

| Code Key         | Notion Property  | Type       |
|------------------|------------------|------------|
| `title`          | Cover            | title      |
| `sourceid`       | SourceId         | rich_text  |
| `send_main`      | SendToMain       | checkbox   |
| `send_alt`       | SendToAlt        | checkbox   |
| `send_archive`   | SendToArchive    | checkbox   |
| `last_synced`    | LastSynced       | date       |

