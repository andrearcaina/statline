from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env")
