from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    READWISE_TOKEN: str
    NOTION_TOKEN: str
    NOTION_DATABASE_ID: str
    MAIN_DB_ID: str | None = None
    ALT_DB_ID: str | None = None
    CATALOG_DB_ID: str
    NOTION_VERSION: str = "2022-06-28"
    RW_UPDATED_AFTER: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
