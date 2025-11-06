import typer, importlib
app = typer.Typer(help="Readwise ↔ Notion")

@app.command()
def run_legacy():
    # Calls old Main 
    m = importlib.import_module("main")
    m.main()  # expects main.py to expose main()

@app.command()
def sync():
    # New modular pipeline (we’ll fill this in as we refactor)
    from .sync.pipeline import run_sync
    print(run_sync())

if __name__ == "__main__":
    app()

