from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    DW_URL: str
    FILE_PATH: str
    CONVERTER_ENV: str
    LOGGING: str