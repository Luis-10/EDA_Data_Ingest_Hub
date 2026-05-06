from sqlalchemy import BigInteger, Boolean, Column, Integer, DateTime, Unicode, UnicodeText

from config.database import SqlServerSacBase


class ss_catalogue_accounts(SqlServerSacBase):
    __tablename__ = "accounts_SAC"
    __table_args__ = {"schema": "catalogue"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Unicode(250), nullable=False)


class ss_catalogue_roles(SqlServerSacBase):
    __tablename__ = "roles_SAC"
    __table_args__ = {"schema": "catalogue"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    role = Column(Unicode(50), nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class ss_catalogue_users(SqlServerSacBase):
    __tablename__ = "users_SAC"
    __table_args__ = {"schema": "catalogue"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(Unicode(100), nullable=False)
    full_name = Column(Unicode(250), nullable=False)
    email = Column(Unicode(250), nullable=False)
    password = Column(Unicode(250), nullable=False)
    create_date_time = Column(DateTime, nullable=False)
    last_connection = Column(DateTime, nullable=True)
    id_role = Column(Integer, nullable=False)
    account = Column(Unicode(250), nullable=True)
    id_account = Column(Integer, nullable=True)
    status = Column(Boolean, nullable=False, default=True)


class ss_catalogue_session_logs(SqlServerSacBase):
    __tablename__ = "session_logs_SAC"
    __table_args__ = {"schema": "catalogue"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_user = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False)
    instruction = Column(Unicode(100), nullable=False)
    sql_command = Column(UnicodeText, nullable=True)
    source = Column(Unicode(250), nullable=True)


class ss_app_auditsa_api_log(SqlServerSacBase):
    __tablename__ = "auditsa_api_log_SAC"
    __table_args__ = {"schema": "app"}

    id = Column(Integer, primary_key=True, autoincrement=False)
    source = Column(Unicode(500), nullable=False)
    date_time = Column(DateTime, nullable=False)
    instruction = Column(Unicode(50), nullable=False)
    total_rows = Column(BigInteger, default=0)
    total_records = Column(BigInteger, default=0)
    status = Column(Unicode(50), nullable=False)


class ss_app_domain_events(SqlServerSacBase):
    __tablename__ = "domain_events_SAC"
    __table_args__ = {"schema": "app"}

    id = Column(Integer, primary_key=True, autoincrement=False)
    event_id = Column(Unicode(36), unique=True, nullable=False)
    event_type = Column(Unicode(100), nullable=False)
    aggregate_type = Column(Unicode(50), nullable=False)
    aggregate_id = Column(Unicode(50), nullable=False)
    payload = Column(UnicodeText, nullable=False)
    occurred_at = Column(DateTime, nullable=False)
    processed_at = Column(DateTime, nullable=True)
    status = Column(Unicode(20), nullable=False, default="PENDING")
    consumer_id = Column(Unicode(100), nullable=True)
