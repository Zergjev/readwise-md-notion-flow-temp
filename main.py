# main.py
# Readwise → Markdown → Notion (upsert), schedulable via GitHub Actions
# Env (required): READWISE_TOKEN, NOTION_TOKEN, NOTION_DATABASE_ID
# Env (optional): MAIN_DB_ID, ALT_DB_ID, CATALOG_DB_ID, RW_UPDATED_AFTER, NOTION_VERSION
# Filters (optional): RW_INCLUDE_TITLES, RW_EXCLUDE_TITLES, RW_INCLUDE_SOURCE_IDS

import os
import re
import time
import json
import pathlib
import datetime
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests


# ====================
# Config / Environment
# ====================
READWISE_TOKEN   = os.getenv("READWISE_TOKEN", "").strip()
NOTION_TOKEN     = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DB_ID     = os.getenv("NOTION_DATABASE_ID", "").strip()  # primary (Main) DB fallback
NOTION_VERSION   = os.getenv("NOTION_VERSION", "2022-06-28").strip()
RW_UPDATED_AFTER = os.getenv("RW_UPDATED_AFTER", "").strip()

# Fixed mapping to your Notion DB column names
DB_SCHEMA: Dict[str, str] = {
    "title"      : "Cover",        # Title
    "highlight"  : "Highlight",    # rich_text
    "note"       : "Note",         # rich_text
    "category"   : "Category",     # select
    "location"   : "Location",     # number
    "url"        : "URL",          # url
    "tags"       : "Tags",         # multi_select
    "author"     : "Author",       # rich_text
    "highlightid": "HighlightId",  # rich_text (upsert key)
}

# Optional repo-secret filters
def _split_csv(name: str) -> List[str]:
    v = os.getenv(name, "")
    return [s.strip() for s in v.split(",") if s.strip()]

RW_INCLUDE_TITLES      : List[str] = _split_csv("RW_INCLUDE_TITLES")
RW_EXCLUDE_TITLES      : List[str] = _split_csv("RW_EXCLUDE_TITLES")
RW_INCLUDE_SOURCE_IDS  : List[str] = _split_csv("RW_INCLUDE_SOURCE_IDS")

if not READWISE_TOKEN:
    raise RuntimeError("Missing env READWISE_TOKEN")
if not NOTION_TOKEN:
    raise RuntimeError("Missing env NOTION_TOKEN")
if not NOTION_DB_ID:
    raise RuntimeError("Missing env NOTION_DATABASE_ID")

READWISE_EXPORT_URL = "https://readwise.io/api/v2/export/"

EXPORT_DIR      = pathlib.Path("exports")
SOURCES_DIR     = EXPORT_DIR / "sources"
CONSOLIDATED_MD = EXPORT_DIR / "readwise_consolidated.md"
LAST_SYNC_FILE  = EXPORT_DIR / ".last_sync_at"

# Properties we warn about if missing (script still proceeds)
REQUIRED_PROPERTIES = ["Cover", "Highlight", "Note", "Category", "Location", "URL"]


# =========
# Utilities
# =========
def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


URL_RE = re.compile(r"^https?://", re.IGNORECASE)

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u if URL_RE.match(u) else "https://" + u


def md_escape(s: str) -> str:
    return (s or "").replace("\r", " ").replace("\n", " ").strip()


def item_fingerprint(item: Dict[str, Any]) -> str:
    """Hash fields we care about so we can skip no-op updates."""
    s = "|".join([
        item.get("title", ""),
        item.get("text", ""),
        item.get("note", ""),
        str(item.get("location") or ""),
        item.get("category", ""),
        normalize_url(item.get("url") or ""),
        ",".join(item.get("tags") or []),
        item.get("author", ""),
    ])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def read_last_sync_iso() -> Optional[str]:
    try:
        return LAST_SYNC_FILE.read_text(encoding="utf-8").strip() or None
    except FileNotFoundError:
        return None


def write_last_sync_iso(ts: str) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    LAST_SYNC_FILE.write_text(ts, encoding="utf-8")


def _match_any_substr(needle: str, haystack_list: List[str]) -> bool:
    if not haystack_list:
        return False
    n = (needle or "").lower()
    return any(p.lower() in n for p in haystack_list)


def include_source(src: Dict[str, Any]) -> bool:
    """Apply include/exclude filters (via repo secrets)."""
    title = (src.get("title") or "").strip()
    sid   = str(src.get("user_book_id") or src.get("source_id") or "").strip()

    # Include lists take priority
    if RW_INCLUDE_SOURCE_IDS and sid not in RW_INCLUDE_SOURCE_IDS:
        return False
    if RW_INCLUDE_TITLES and not _match_any_substr(title, RW_INCLUDE_TITLES):
        return False

    # Exclusions apply afterwards
    if RW_EXCLUDE_TITLES and _match_any_substr(title, RW_EXCLUDE_TITLES):
        return False
    return True


def tag_names_list(raw_tags: Any) -> List[str]:
    """Normalize tag structures to plain tag names."""
    if not raw_tags:
        return []
    out: List[str] = []
    for t in raw_tags:
        if isinstance(t, dict):
            if isinstance(t.get("name"), str):
                out.append(t["name"])
            elif isinstance(t.get("title"), str):
                out.append(t["title"])
            else:
                out.append(str(t))
        else:
            out.append(str(t))
    return out


# ===========
# Notion HTTP
# ===========
def notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_request(method: str, url: str, **kwargs) -> requests.Response:
    """Notion API with simple 429 backoff + error prints."""
    for attempt in range(8):
        r = requests.request(method, url, headers=notion_headers(), timeout=60, **kwargs)
        if r.status_code != 429:
            if r.status_code >= 400:
                try:
                    print("Notion error:", json.dumps(r.json(), ensure_ascii=False))
                except Exception:
                    print("Notion error status:", r.status_code, "body:", r.text[:500])
                r.raise_for_status()
            return r
        retry_after = int(r.headers.get("Retry-After", "2"))
        sleep_s = min(2 ** attempt, 16) + retry_after
        time.sleep(sleep_s)
    r.raise_for_status()
    return r


def ensure_db_has_required_properties(db_id: str) -> None:
    """Warn if any required property is missing (won't fail the run)."""
    r = notion_request("GET", f"https://api.notion.com/v1/databases/{db_id}")
    db = r.json()
    props = db.get("properties", {})
    missing = [p for p in REQUIRED_PROPERTIES if p not in props]
    if missing:
        print("WARNING: Your Notion DB is missing properties:", ", ".join(missing))
        print("         The script will create rows using the properties that exist.")


# ================================================
# Catalog helpers (matches your "Kindle Book Catalog")
# ================================================
CATALOG_SCHEMA: Dict[str, str] = {
    "title"       : "Cover",        # your Title column is named "Cover"
    "sourceid"    : "SourceId",
    "send_main"   : "SendToMain",
    "send_alt"    : "SendToAlt",
    "send_archive": "SendToArchive",
    "last_synced" : "LastSynced",
}

def _rt_plain(props: Dict[str, Any], name: str) -> str:
    rt = props.get(name, {}).get("rich_text", [])
    if rt and isinstance(rt, list):
        first = rt[0] or {}
        return (first.get("plain_text") or "").strip()
    return ""

def _title_plain(props: Dict[str, Any], name: str) -> str:
    tt = props.get(name, {}).get("title", [])
    if tt and isinstance(tt, list):
        first = tt[0] or {}
        return (first.get("plain_text") or "").strip()
    return ""

def _checkbox(props: Dict[str, Any], name: str) -> bool:
    return bool(props.get(name, {}).get("checkbox", False))

def catalog_index(db_id: str) -> Dict[str, Dict[str, Any]]:
    """Return { SourceId: {page_id, title, send_main, send_alt, send_archive} }"""
    idx: Dict[str, Dict[str, Any]] = {}
    cursor: Optional[str] = None

    while True:
        payload: Dict[str, Any] = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor

        r = notion_request("POST", f"https://api.notion.com/v1/databases/{db_id}/query", json=payload)
        j = r.json()

        for page in j.get("results", []):
            props = page.get("properties", {})
            sid = _rt_plain(props, CATALOG_SCHEMA["sourceid"])  # MUST exist
            if not sid:
                continue
            idx[sid] = {
                "page_id"     : page.get("id", ""),
                "title"       : _title_plain(props, CATALOG_SCHEMA["title"]),
                "send_main"   : _checkbox(props, CATALOG_SCHEMA["send_main"]),
                "send_alt"    : _checkbox(props, CATALOG_SCHEMA["send_alt"]),
                "send_archive": _checkbox(props, CATALOG_SCHEMA["send_archive"]),
            }

        if j.get("has_more"):
            cursor = j.get("next_cursor")
            time.sleep(0.15)
        else:
            break

    return idx

def seed_catalog(catalog_db_id: str, sources: List[Dict[str, Any]]) -> None:
    """
    Create rows for any sources missing in the catalog.
    Sets checkboxes to False by default and stamps LastSynced.
    Also updates LastSynced on existing rows.
    """
    if not catalog_db_id:
        return

    idx = catalog_index(catalog_db_id)
    now = now_iso()

    for src in sources:
        sid = str(src.get("user_book_id") or src.get("source_id") or "").strip()
        if not sid:
            continue
        title = (src.get("title") or "Untitled").strip()

        if sid in idx:
            page_id = idx[sid]["page_id"]
            notion_request(
                "PATCH",
                f"https://api.notion.com/v1/pages/{page_id}",
                json={"properties": {CATALOG_SCHEMA["last_synced"]: {"date": {"start": now}}}},
            )
        else:
            props = {
                CATALOG_SCHEMA["title"]       : {"title": [{"type": "text", "text": {"content": title}}]},
                CATALOG_SCHEMA["sourceid"]    : {"rich_text": [{"type": "text", "text": {"content": sid}}]},
                CATALOG_SCHEMA["send_main"]   : {"checkbox": False},
                CATALOG_SCHEMA["send_alt"]    : {"checkbox": False},
                CATALOG_SCHEMA["send_archive"]: {"checkbox": False},
                CATALOG_SCHEMA["last_synced"] : {"date": {"start": now}},
            }
            notion_request(
                "POST",
                "https://api.notion.com/v1/pages",
                json={"parent": {"database_id": catalog_db_id}, "properties": props},
            )

def load_catalog_filters(catalog_db_id: str) -> Dict[str, Dict[str, bool]]:
    """Return { SourceId: {"send_main": bool, "send_alt": bool, "send_archive": bool} }"""
    out: Dict[str, Dict[str, bool]] = {}
    if not catalog_db_id:
        return out
    idx = catalog_index(catalog_db_id)
    for sid, row in idx.items():
        out[sid] = {
            "send_main"   : bool(row.get("send_main", False)),
            "send_alt"    : bool(row.get("send_alt", False)),
            "send_archive": bool(row.get("send_archive", False)),
        }
    return out


# ==============================
# Notion index (for highlight upsert)
# ==============================
def load_existing_index(db_id: str) -> Dict[str, Dict[str, str]]:
    """Return {highlight_id: {"page_id":..., "fingerprint":...}}"""
    if not DB_SCHEMA.get("highlightid"):
        return {}

    idx: Dict[str, Dict[str, str]] = {}
    cursor: Optional[str] = None

    while True:
        payload: Dict[str, Any] = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = notion_request("POST", f"https://api.notion.com/v1/databases/{db_id}/query", json=payload)
        j = r.json()

        for page in j.get("results", []):
            props = page.get("properties", {})
            rt = props.get(DB_SCHEMA["highlightid"], {}).get("rich_text", [])
            hid = (rt[0].get("plain_text").strip() if rt and rt[0].get("plain_text") else "")
            if not hid:
                continue

            def get_rt(name: str) -> str:
                v = props.get(name, {}).get("rich_text", [])
                return v[0].get("plain_text") if v else ""

            def get_sel(name: str) -> str:
                s = props.get(name, {}).get("select", {})
                return s.get("name") or ""

            def get_num(name: str) -> str:
                n = props.get(name, {}).get("number")
                return str(n) if n is not None else ""

            def get_url(name: str) -> str:
                return props.get(name, {}).get("url") or ""

            def get_tags(name: str) -> str:
                ms = props.get(name, {}).get("multi_select", [])
                return ",".join([t.get("name", "") for t in ms])

            existing = {
                "title"   : get_rt(DB_SCHEMA["title"])      if DB_SCHEMA.get("title")      else "",
                "text"    : get_rt(DB_SCHEMA["highlight"])  if DB_SCHEMA.get("highlight")  else "",
                "note"    : get_rt(DB_SCHEMA["note"])       if DB_SCHEMA.get("note")       else "",
                "category": get_sel(DB_SCHEMA["category"])  if DB_SCHEMA.get("category")   else "",
                "location": get_num(DB_SCHEMA["location"])  if DB_SCHEMA.get("location")   else "",
                "url"     : get_url(DB_SCHEMA["url"])       if DB_SCHEMA.get("url")        else "",
                "tags"    : get_tags(DB_SCHEMA["tags"])     if DB_SCHEMA.get("tags")       else "",
                "author"  : get_rt(DB_SCHEMA["author"])     if DB_SCHEMA.get("author")     else "",
            }
            fp = hashlib.sha256("|".join(existing.values()).encode("utf-8")).hexdigest()
            idx[hid] = {"page_id": page["id"], "fingerprint": fp}

        if j.get("has_more"):
            cursor = j["next_cursor"]
            time.sleep(0.15)
        else:
            break

    return idx


def build_props(item: Dict[str, Any]) -> Dict[str, Any]:
    """Translate a row dict into Notion page properties."""
    s = DB_SCHEMA
    props: Dict[str, Any] = {}

    props[s["title"]] = {
        "title": [{"type": "text", "text": {"content": item.get("title") or "Untitled"}}]
    }
    if s.get("highlight"):
        props[s["highlight"]] = {"rich_text": [{"type": "text", "text": {"content": item.get("text") or ""}}]}
    if s.get("note"):
        note_val = item.get("note") or ""
        props[s["note"]] = {"rich_text": ([{"type": "text", "text": {"content": note_val}}] if note_val else [])}
    if s.get("category"):
        props[s["category"]] = {"select": {"name": (item.get("category") or "Books")}}
    if s.get("location"):
        if item.get("location") is None:
            props[s["location"]] = {"number": None}
        else:
            try:
                props[s["location"]] = {"number": float(item["location"])}
            except Exception:
                props[s["location"]] = {"number": None}
    if s.get("url"):
        u = normalize_url(item.get("url") or "")
        props[s["url"]] = {"url": u if u else None}
    if s.get("tags") and item.get("tags"):
        props[s["tags"]] = {"multi_select": [{"name": t} for t in item["tags"]]}
    if s.get("author"):
        props[s["author"]] = {"rich_text": [{"type": "text", "text": {"content": item.get("author") or ""}}]}
    if s.get("highlightid") and item.get("highlight_id"):
        props[s["highlightid"]] = {"rich_text": [{"type": "text", "text": {"content": item["highlight_id"]}}]}
    return props


def upsert_all(db_id: str, rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """Upsert a batch of rows into the specified database."""
    index = load_existing_index(db_id)
    created = updated = skipped = 0

    for item in rows:
        hid = item.get("highlight_id", "")
        fp  = item_fingerprint(item)
        props = build_props(item)

        if hid and hid in index:
            page = index[hid]
            if page.get("fingerprint") == fp:
                skipped += 1
                continue
            notion_request("PATCH", f"https://api.notion.com/v1/pages/{page['page_id']}", json={"properties": props})
            updated += 1
        else:
            notion_request("POST", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
            created += 1

    return created, updated, skipped


# ========
# Readwise
# ========
def fetch_readwise(updated_after: Optional[str]) -> List[Dict[str, Any]]:
    """Readwise Export API (v2) with pagination."""
    headers = {"Authorization": f"Token {READWISE_TOKEN}"}
    params: Dict[str, Any] = {}
    if updated_after:
        params["updatedAfter"] = updated_after

    out: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None
    while True:
        if next_cursor:
            params["pageCursor"] = next_cursor
        r = requests.get(READWISE_EXPORT_URL, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        out.extend(j.get("results", []))
        next_cursor = j.get("nextPageCursor")
        if not next_cursor:
            break
    return out


# ======================
# Markdown (optional out)
# ======================
def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9\-_. ]+", "", text)
    text = re.sub(r"\s+", "-", text)
    return text[:80] or "untitled"

def render_markdown(results: List[Dict[str, Any]]) -> Dict[str, str]:
    """Return {"consolidated": md, "per_source": {"<slug>.md": content}}."""
    consolidated_lines = ["# Readwise Export", f"Generated: {now_iso()}"]
    per_source: Dict[str, str] = {}

    for src in results:
        title_dbg = src.get("title")
        sid_dbg   = src.get("user_book_id") or src.get("source_id")
        print(f"Checking: {title_dbg} (ID: {sid_dbg})")

        if not include_source(src):
            print(f"— EXCLUDED from MD: {title_dbg}")
            continue
        print(f"— INCLUDED in MD: {title_dbg}")

        title    = src.get("title") or "Untitled"
        author   = src.get("author") or ""
        category = src.get("category") or ""

        consolidated_lines.append("")
        consolidated_lines.append(f"## {title}" + (f" — {author}" if author else ""))
        if category:
            consolidated_lines.append(f"*Category:* {category}")

        lines = [f"# {title}" + (f" — {author}" if author else "")]
        if category:
            lines.append(f"_Category:_ {category}")
        lines.append("")

        for h in (src.get("highlights") or []):
            h_text = md_escape(h.get("text") or "")
            loc    = h.get("location")
            note   = md_escape(h.get("note") or "")
            tags   = tag_names_list(h.get("tags") or [])
            url    = h.get("url") or src.get("source_url") or ""
            hl_at  = h.get("highlighted_at") or ""

            bullet = f"- {h_text}"
            if loc:
                bullet += f" _(loc {loc})_"
            consolidated_lines.append(bullet)
            if note: consolidated_lines.append(f"  \n  ↳ **Note:** {note}")
            if tags: consolidated_lines.append(f"  \n  ↳ *Tags:* {', '.join(tags)}")
            if url:  consolidated_lines.append(f"  \n  ↳ {url}")
            if hl_at: consolidated_lines.append(f"  \n  ↳ _Highlighted at:_ {hl_at}")

            lines.append(bullet)
            if note: lines.append(f"  \n  ↳ **Note:** {note}")
            if tags: lines.append(f"  \n  ↳ *Tags:* {', '.join(tags)}")
            if url:  lines.append(f"  \n  ↳ {url}")
            if hl_at: lines.append(f"  \n  ↳ _Highlighted at:_ {hl_at}")

        per_source[f"{slugify(title)}.md"] = "\n".join(lines)

    return {"consolidated": "\n".join(consolidated_lines), "per_source": per_source}


# ===============================================
# Normalize rows (one row per highlight for Notion)
# ===============================================
def flatten_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Turn Readwise export into per-highlight rows for Notion upsert,
    carrying forward the chosen target database (if any).
    """
    rows: List[Dict[str, Any]] = []
    for src in results:
        if not include_source(src):
            continue

        base = {
            "title"     : src.get("title") or "",
            "author"    : src.get("author") or "",
            "category"  : src.get("category") or "",
            "source_id" : str(src.get("user_book_id") or src.get("source_id") or ""),
            "source_url": src.get("source_url") or "",
            "target_db" : src.get("target_db"),  # set during routing
        }

        for h in (src.get("highlights") or []):
            rows.append({
                **base,
                "highlight_id" : str(h.get("id") or ""),
                "text"         : h.get("text") or "",
                "note"         : h.get("note") or "",
                "location"     : h.get("location"),
                "tags"         : tag_names_list(h.get("tags") or []),
                "url"          : h.get("url") or base["source_url"],
                "highlighted_at": h.get("highlighted_at"),
            })
    return rows


# ====
# Main
# ====
def main() -> None:
    # ---------- Setup ----------
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)

    # DB IDs
    main_db_id    = os.getenv("MAIN_DB_ID", "").strip() or NOTION_DB_ID
    alt_db_id     = os.getenv("ALT_DB_ID", "").strip() or None
    catalog_db_id = os.getenv("CATALOG_DB_ID", "").strip() or None

    # Schema sanity
    ensure_db_has_required_properties(main_db_id)
    if alt_db_id:
        try:
            ensure_db_has_required_properties(alt_db_id)
        except Exception as e:
            print(f"⚠️ Could not verify Alt DB schema: {e}")

    # Info banner
    print(f"→ Main DB ID: {main_db_id}")
    print(f"→ Alt  DB ID: {alt_db_id or '(none)'}")
    print(f"→ Catalog DB ID: {catalog_db_id or '(none)'}")

    # Filters banner
    if RW_INCLUDE_TITLES:
        print(f"✅ Including titles matching: {RW_INCLUDE_TITLES}")
    else:
        print("⚙️ No title-include filter.")
    if RW_EXCLUDE_TITLES:
        print(f"🚫 Excluding titles matching: {RW_EXCLUDE_TITLES}")
    if RW_INCLUDE_SOURCE_IDS:
        print(f"✅ Including only source IDs: {RW_INCLUDE_SOURCE_IDS}")

    # ---------- Catalog (optional) ----------
    catalog = None
    if catalog_db_id:
        try:
            catalog = load_catalog_filters(catalog_db_id)
            print(f"📒 Loaded {len(catalog)} catalog entries.")
        except Exception as e:
            print(f"⚠️ Catalog load failed: {e}")
            catalog = None

    # ---------- Fetch from Readwise ----------
    print("Fetching Readwise export...")
    updated_after = None  # use read_last_sync_iso() for incremental after first full run
    print(f"📅 Sync starting. Updated after: {updated_after or 'FULL SYNC'}")
    results = fetch_readwise(updated_after)
    print(f"📚 Retrieved {len(results)} sources from Readwise.")

    # ---------- Title-level merge/dedupe ----------
    def merge_sources_by_title(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for src in sources:
            tkey = (src.get("title") or "").strip().lower()
            if not tkey:
                tkey = f"__untitled__::{src.get('user_book_id') or src.get('source_id') or ''}"

            if tkey not in merged:
                base = dict(src)
                base["highlights"] = list(src.get("highlights") or [])
                merged[tkey] = base
                continue

            dest = merged[tkey]
            dest_hls = {str(h.get("id")): h for h in (dest.get("highlights") or [])}
            for h in (src.get("highlights") or []):
                hid = str(h.get("id") or "")
                if not hid:
                    continue
                if hid in dest_hls:
                    if not dest_hls[hid].get("note") and h.get("note"):
                        dest_hls[hid]["note"] = h.get("note")
                    if h.get("tags"):
                        existing_tags = set(tag_names_list(dest_hls[hid].get("tags") or []))
                        new_tags = set(tag_names_list(h.get("tags") or []))
                        dest_hls[hid]["tags"] = [{"name": t} for t in sorted(existing_tags | new_tags)]
                else:
                    dest_hls[hid] = h
            dest["highlights"] = sorted(dest_hls.values(), key=lambda x: (x.get("location") is None, x.get("location")))
        return list(merged.values())

    results = merge_sources_by_title(results)
    print(f"📘 After title-level merge: {len(results)} unique titles remain.")

    # ---------- Seed the Catalog DB ----------
    if catalog_db_id:
        try:
            print("📒 Seeding catalog database with any missing books...")
            seed_catalog(catalog_db_id, results)
            print("✅ Catalog sync complete.")
        except Exception as e:
            print(f"⚠️ Catalog sync failed: {e}")
    else:
        print("ℹ️ No CATALOG_DB_ID provided; skipping catalog seeding.")

    # ---------- Catalog routing (decide target DB per source) ----------
    if catalog:
        routed: List[Dict[str, Any]] = []
        for src in results:
            sid = str(src.get("user_book_id") or src.get("source_id") or "").strip()
            prefs = catalog.get(sid)
            if not prefs:
                print(f"📒 Not in catalog (skip): {src.get('title')}")
                continue
            if prefs.get("send_archive"):
                print(f"🗃️ Archived (skip): {src.get('title')}")
                continue

            target = None
            if prefs.get("send_main"):
                target = main_db_id
            elif prefs.get("send_alt") and alt_db_id:
                target = alt_db_id

            if not target:
                print(f"🚫 Excluded by catalog: {src.get('title')}")
                continue

            src["target_db"] = target
            label = "Main DB" if target == main_db_id else "Alt DB"
            print(f"✅ Routing {src.get('title')} → {label}")
            routed.append(src)

        results = routed
    else:
        # No catalog: route everything to the main DB
        for src in results:
            src["target_db"] = main_db_id

    if not results:
        done_at = now_iso()
        print("No sources to process after catalog filters; nothing to do.")
        print(f"SYNC_COMPLETE_AT={done_at}")
        write_last_sync_iso(done_at)
        return

    # ---------- Markdown outputs (optional) ----------
    md = render_markdown(results)
    CONSOLIDATED_MD.write_text(md["consolidated"], encoding="utf-8")
    for fname, content in md["per_source"].items():
        (SOURCES_DIR / fname).write_text(content, encoding="utf-8")

    # ---------- Apply include/exclude (secrets) ----------
    filtered_sources: List[Dict[str, Any]] = []
    for src in results:
        title = (src.get("title") or "").strip()
        sid   = str(src.get("user_book_id") or src.get("source_id") or "").strip()
        if not include_source(src):
            print(f"— EXCLUDED by secrets: {title} [{sid}]")
            continue
        print(f"— INCLUDED: {title} [{sid}]")
        filtered_sources.append(src)

    if not filtered_sources:
        done_at = now_iso()
        print("No sources after include/exclude filters; nothing to do.")
        print(f"SYNC_COMPLETE_AT={done_at}")
        write_last_sync_iso(done_at)
        return

    # ---------- Split routing (Main vs Alt) ----------
    main_sources: List[Dict[str, Any]] = []
    alt_sources : List[Dict[str, Any]] = []

    for s in filtered_sources:
        target = s.get("target_db")
        if target == alt_db_id:
            alt_sources.append(s)
        elif target == main_db_id or not target:
            main_sources.append(s)

    print(f"🧭 Routing summary: {len(main_sources)} → Main, {len(alt_sources)} → Alt")

    # Combine for downstream processing (order doesn’t matter)
    filtered_sources = main_sources + alt_sources

    # ---------- Flatten to row dicts and carry target DB ----------
    rows = flatten_rows(filtered_sources)

    # Carry target_db into each row using the source_id
    target_map = {
        str(s.get("user_book_id") or s.get("source_id") or ""): s.get("target_db")
        for s in filtered_sources
    }
    for r in rows:
        r["target_db"] = target_map.get(r.get("source_id") or "") or main_db_id

    # Optional preview: which Alt source IDs we’d remove from Main (preview only)
    if alt_sources and main_db_id:
        alt_ids = {str(s.get("source_id") or s.get("user_book_id")) for s in alt_sources}
        print(f"🧹 Would remove {len(alt_ids)} Alt entries from Main DB (preview)")

    # ---------- Write to Notion (bucket by DB) ----------
    total = len(rows)
    missing_hid = sum(1 for r in rows if not r.get("highlight_id"))
    print(f"Rows to write: {total}. Missing highlight_id: {missing_hid}")
    if total and rows[0].get("highlight_id"):
        print(f"Example highlight_id: {rows[0]['highlight_id']}")

    print("Writing rows to Notion...")
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        dbid = r.get("target_db") or main_db_id
        buckets.setdefault(dbid, []).append(r)

    print("Routing summary:")
    for dbid, bucket in buckets.items():
        label = "Main DB" if dbid == main_db_id else ("Alt DB" if alt_db_id and dbid == alt_db_id else dbid)
        print(f"  • {label}: {len(bucket)} rows")

    created = updated = skipped = 0
    for dbid, bucket in buckets.items():
        c, u, s = upsert_all(dbid, bucket)
        created += c; updated += u; skipped += s
        label = "Main DB" if dbid == main_db_id else ("Alt DB" if alt_db_id and dbid == alt_db_id else dbid)
        print(f"  • {label}: created {c}, updated {u}, skipped {s}")

    print(f"Done. Created {created}, updated {updated}, skipped {skipped}.")

    # ---------- Persist sync marker ----------
    done_at = now_iso()
    print(f"SYNC_COMPLETE_AT={done_at}")
    write_last_sync_iso(done_at)


# keep at bottom
if __name__ == "__main__":
    main()
