from sqlalchemy import Column, Integer, BigInteger, String, DateTime
from sqlalchemy.orm import relationship
from config.database import Base


class db_catalogue_roles(Base):
    __tablename__ = "catalogue_roles"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    role = Column(String(50), nullable=False)  # nombre del rol
    status = Column(String(50), nullable=False)  # RECORDING | SUCCESS


class db_catalogue_user(Base):
    __tablename__ = "catalogue_users"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    user = Column(String(100), nullable=False)  # nombre del usuario
    full_name = Column(String(250), nullable=False)  # nombre completo del usuario
    email = Column(String(250), nullable=False)  # correo electrónico del usuario
    password_hash = Column(
        String(250), nullable=False
    )  # hash de la contraseña del usuario
    create_date_time = Column(
        DateTime, nullable=False
    )  # timestamp de creación del usuario (inmutable)
    update_date_time = Column(
        DateTime, nullable=True
    )  # timestamp de última actualización del usuario
    id_role = Column(Integer, nullable=False)  # ID del rol del usuario (FK a catalogue_roles.id)

    # Relación con db_catalogue_roles (opcional, para facilitar consultas)
    role = relationship("db_catalogue_roles", backref="users")

    status = Column(String(50), nullable=False)  # RECORDING | SUCCESS
