from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    log_level: str = "INFO"  # default log level if not specified in .env file

    model_config = SettingsConfigDict(env_file=".env")
