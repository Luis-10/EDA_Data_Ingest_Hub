from sqlalchemy import create_engine, MetaData, exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

from config.settings import get_settings

Base = declarative_base()
SqlServerBase = declarative_base()
SqlServerSacBase = declarative_base()

settings = get_settings()

# ── PostgreSQL ────────────────────────────────────────────────────────────────
engine: Engine = create_engine(
    settings.get_database_url(),
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_recycle=1800,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── SQL Server (externo) ──────────────────────────────────────────────────────
sqlserver_engine: Engine = create_engine(
    settings.get_sqlserver_url(),
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_recycle=1800,
)
SqlServerSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlserver_engine)


def get_sqlserver_db() -> Session:
    db = SqlServerSessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── SQL Server mxTarifas ──────────────────────────────────────────────────────
sqlserver_engine_tarifas: Engine = create_engine(
    settings.get_sqlserver_tarifas_url(),
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_recycle=1800,
)
SqlServerTarifasSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlserver_engine_tarifas)


def get_sqlserver_tarifas_db() -> Session:
    db = SqlServerTarifasSessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── SQL Server appSAC (catálogos de usuarios) ─────────────────────────────────
sqlserver_engine_sac: Engine = create_engine(
    settings.get_sqlserver_sac_url(),
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_recycle=1800,
)
SqlServerSacSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlserver_engine_sac)


def get_sqlserver_sac_db() -> Session:
    db = SqlServerSacSessionLocal()
    try:
        yield db
    finally:
        db.close()


class DatabaseException(Exception):
    pass


def handle_db_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exc.SQLAlchemyError as e:
            raise DatabaseException(f"Database error: {str(e)}")

    return wrapper
