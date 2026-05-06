from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

"""
Configuración de la aplicación.
En este archivo se definen las variables de entorno necesarias para la configuración de la aplicación.
"""


class Settings(BaseSettings):
    #  PostgreSQL
    database_url: str = ""
    postgres_user: str = "admin"
    postgres_password: str = ""
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "datahub_db"

    #  SQL Server
    sqlserver_host: str = ""
    sqlserver_user: str = ""
    sqlserver_password: str = ""
    sqlserver_db: str = "master"
    sqlserver_port: int = 1433
    sqlserver_db_tarifas: str = "mxTarifas"
    sqlserver_db_sac: str = "appSAC"

    #  AWS / LocalStack 
    aws_endpoint_url: str = "http://localhost:4566"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    aws_region: str = "us-east-1"
    s3_bucket: str = "raw-data"
    sqs_base_url: str = "http://localhost:4566/000000000000"

    #  Auditsa API 
    auditsa_url: str = ""
    auditsa_token: str = ""
    auditsa_clid: str = ""
    str_start: str = "000000"
    str_end: str = "235959"

    #  ETL 
    etl_parallel_workers: int = 5

    #  JWT 
    jwt_secret_key: str = "change-me-in-production-use-a-long-random-string"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24 h

    #  Usuarios 
    admin_username: str = "admin"
    admin_password: str = "admin123"
    reader_username: str = "reader"
    reader_password: str = "reader123"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    def get_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        from urllib.parse import quote_plus

        return (
            f"postgresql+psycopg://{self.postgres_user}:"
            f"{quote_plus(self.postgres_password)}@"
            f"{self.postgres_host}:{self.postgres_port}/"
            f"{self.postgres_db}"
        )

    def get_sqlserver_url(self) -> str:
        from urllib.parse import quote_plus

        return (
            f"mssql+pymssql://{self.sqlserver_user}:"
            f"{quote_plus(self.sqlserver_password)}@"
            f"{self.sqlserver_host}:{self.sqlserver_port}/"
            f"{self.sqlserver_db}"
        )

    def get_sqlserver_tarifas_url(self) -> str:
        from urllib.parse import quote_plus

        return (
            f"mssql+pymssql://{self.sqlserver_user}:"
            f"{quote_plus(self.sqlserver_password)}@"
            f"{self.sqlserver_host}:{self.sqlserver_port}/"
            f"{self.sqlserver_db_tarifas}"
        )

    def get_sqlserver_sac_url(self) -> str:
        from urllib.parse import quote_plus

        return (
            f"mssql+pymssql://{self.sqlserver_user}:"
            f"{quote_plus(self.sqlserver_password)}@"
            f"{self.sqlserver_host}:{self.sqlserver_port}/"
            f"{self.sqlserver_db_sac}"
        )


#  Obtiene la configuración de la aplicación. 
@lru_cache
def get_settings() -> Settings:
    return Settings()
