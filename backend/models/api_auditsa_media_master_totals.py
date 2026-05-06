from sqlalchemy import Column, Integer, String, BigInteger, Float, Date, Text
from config.database import Base


class db_auditsa_media_master_totals(Base):
    __tablename__ = "auditsa_api_media_master_totals"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    FechaInicio = Column(Date)
    FechaFin = Column(Date)
    Periodo = Column(String(100))
    Medio = Column(String(128))
    Canal = Column(String(255))
    GEstacion = Column(String(255))
    FHoraria = Column(String(100))
    NoCorte = Column(Integer)
    TipoSpot = Column(String(255))
    Genero = Column(String(255))
    TipoMaster = Column(String(128))
    IdMaster = Column(BigInteger)
    Version = Column(Text)
    Anunciante = Column(String(255))
    Marca = Column(String(255))
    Submarca = Column(String(255))
    Producto = Column(String(255))
    Segmento = Column(String(255))
    Industria = Column(String(255))
    Mercado = Column(String(255))
    DTeorica = Column(Integer)
    TotalHit = Column(Integer)
    TotalInversion = Column(Float)
    Testigo = Column(Text, default="N.A.", server_default="N.A.")
