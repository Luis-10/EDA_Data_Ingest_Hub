from sqlalchemy import Column, Integer, String, BigInteger, Text
from config.database import Base


class db_auditsa_media_master_library(Base):
    __tablename__ = "auditsa_api_media_master_library"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    IdMaster = Column(BigInteger)
    TipoMaster = Column(String(128))
    Version = Column(Text)
    Anunciante = Column(String(255))
    Marca = Column(String(255))
    Submarca = Column(String(255))
    Producto = Column(String(255))
    Segmento = Column(String(255))
    Industria = Column(String(255))
    Mercado = Column(String(255))
    DTeorica = Column(Integer)
    Testigo = Column(Text, default="N.A.", server_default="N.A.")
