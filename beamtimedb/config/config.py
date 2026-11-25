from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# List all environment variables to import from .env and their types. Secrets are typed as SecretStr.
class Settings(BaseSettings):
    db_uname: SecretStr
    db_pwd: SecretStr
    db_host: str = "localhost"
    db_name: str
    db_port: int
    database_uri: SecretStr

    # Define file path from project root for .env file.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")