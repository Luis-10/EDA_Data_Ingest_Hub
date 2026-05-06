from sqlalchemy import Column, Integer, String, BigInteger, Float, Date, Text
from config.database import Base


class db_auditsa_impresos(Base):
    __tablename__ = "auditsa_api_impresos"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    IdMedio = Column(Integer)
    Medio = Column(String(128))
    IdFuente = Column(Integer, default=0, server_default='0')
    Fuente = Column(String(128))
    Autor = Column(String(128))
    Seccion = Column(String(128))
    Pagina = Column(String(128))
    Tiraje = Column(BigInteger)
    Costo = Column(Float)
    Fecha = Column(Date)
    Testigo = Column(Text, default='N.A.', server_default='N.A.')
    Anunciante = Column(String(128))
    Marca = Column(String(128))
    Submarca = Column(String(128))
    Producto = Column(String(128))
    Sector = Column(String(128))
    Subsector = Column(String(128))
    Categoria = Column(String(128))
    Dimension = Column(Float)
    TextoNota = Column(Text, default='N.A.', server_default='N.A.')
