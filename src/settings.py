from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="../config/.env", extra="ignore")
    DW_URL: str = 'https://staging.laji.fi/laji-etl'
    FILE_PATH: str = 'test_data/'
    OUTPUT_PATH: str = ''
    CONVERTER_ENV: str = 'staging'
    LOGGING: str = 'DEBUG'
    
    # Email settings
    SMTP_HOST: str = 'localhost'
    SMTP_PORT: int = 25
    SMTP_USER: str = 'user'
    SMTP_PASSWORD: str = 'password'
    SMTP_TLS: bool = True
    FROM_EMAIL: str = 'geoconverter@luomus.fi'
    ADMIN_EMAILS: str = 'firstname@lastname.fi'
    EMAIL_ENABLED: bool = False