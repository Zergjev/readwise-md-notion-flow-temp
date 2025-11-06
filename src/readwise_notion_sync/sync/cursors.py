import json, pathlib, time
CURSOR_PATH = pathlib.Path(".last_sync.json")

def read_cursor() -> str | None:
    if CURSOR_PATH.exists():
        return json.loads(CURSOR_PATH.read_text()).get("updated_after")
    return None

def write_cursor(ts: str | None = None):
    CURSOR_PATH.write_text(
        json.dumps({"updated_after": ts or time.strftime("%Y-%m-%dT%H:%M:%SZ")})
    )

