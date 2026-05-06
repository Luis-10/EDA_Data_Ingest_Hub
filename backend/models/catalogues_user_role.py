from sqlalchemy import Boolean, Column, Integer, String, DateTime, Text, ForeignKey
from config.database import Base


class db_catalogue_roles(Base):
    __tablename__ = "catalogue_roles"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    role = Column(String(50), nullable=False)
    status = Column(Boolean, nullable=False, default=True)


class db_catalogue_user(Base):
    __tablename__ = "catalogue_users"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    user = Column(String(100), nullable=False)
    full_name = Column(String(250), nullable=False)
    email = Column(String(250), nullable=False)
    password_hash = Column(String(250), nullable=False)
    create_date_time = Column(DateTime, nullable=False)
    last_connection = Column(DateTime, nullable=True)
    update_date_time = Column(DateTime, nullable=True)
    id_role = Column(Integer, ForeignKey("public.catalogue_roles.id"), nullable=False)
    account = Column(String(250), nullable=True)
    id_account = Column(Integer, nullable=True)
    status = Column(Boolean, nullable=False, default=True)


class db_session_logs(Base):
    __tablename__ = "catalogue_session_logs"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    id_user = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False)
    instruction = Column(String(100), nullable=False)
    sql_command = Column(Text, nullable=True)
    source = Column(String(250), nullable=True)
