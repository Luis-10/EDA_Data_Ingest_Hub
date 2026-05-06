from sqlalchemy import Column, Integer, BigInteger, String, DateTime
from config.database import Base


class db_auditsa_api_log(Base):
    __tablename__ = "auditsa_api_log"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    source = Column(String(500), nullable=False)       # S3 path o nombre de tabla DB
    date_time = Column(DateTime, nullable=False)       # timestamp de inicio de operación
    instruction = Column(String(50), nullable=False)   # INSERT | DELETE
    total_rows = Column(BigInteger, default=0)         # total filas extraídas de la fuente
    total_records = Column(BigInteger, default=0)      # filas efectivamente procesadas
    status = Column(String(50), nullable=False)        # RECORDING | SUCCESS
