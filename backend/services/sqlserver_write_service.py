"""
Servicio de escritura dual hacia SQL Server mxTarifas.
Se llama después del commit a PostgreSQL en cada ETL service.
Si SQL Server falla, el error es no-fatal: el commit a Postgres ya está hecho.
"""

import logging
import math
from datetime import date, datetime
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import text

from config.database import SqlServerTarifasSessionLocal

logger = logging.getLogger(__name__)

_BATCH_SIZE = 1000

_TV_RADIO_COLS = [
    "Fecha", "IdPlaza", "Plaza", "IdLocalidad", "Localidad",
    "IdMedio", "Medio", "IdTipoMedio", "TipoMedio", "IdCanal", "Canal",
    "HInicio", "HFinal", "IdMarca", "Marca", "IdSubmarca", "Submarca",
    "IdProducto", "Producto", "IdVersion", "Version", "DReal", "DTeorica",
    "IdTipoSpot", "TipoSpot", "IdSubTipoSpot", "SubTipoSpot",
    "IdTipoCorte", "TipoCorte", "NoCorte", "PEnCorte",
    "IdCampania", "Campania", "IdCorporativo", "Corporativo",
    "IdAnunciante", "Anunciante", "IdAgenciaP", "AgenciaP",
    "IdCentralMedios", "CentralMedios", "IdIndustria", "Industria",
    "IdMercado", "Mercado", "IdSegmento", "Segmento",
    "IdDeteccion", "Tarifa", "Testigo", "IdPrograma", "Programa",
    "Subtitulo", "IdGenero", "Genero", "FHoraria", "GComercial",
    "GEstacion", "Origen", "VerCodigo",
]

_IMPRESOS_COLS = [
    "IdMedio", "Medio", "IdFuente", "Fuente", "Autor", "Seccion", "Pagina",
    "Tiraje", "Costo", "Fecha", "Testigo", "Anunciante", "Marca", "Submarca",
    "Producto", "Sector", "Subsector", "Categoria", "Dimension", "TextoNota",
]

_MEDIA_MASTER_COLS = [
    "IdMaster", "TipoMaster", "Version", "Anunciante", "Marca", "Submarca",
    "Producto", "Segmento", "Industria", "Mercado", "DTeorica", "Testigo",
]

_MEDIA_MASTER_TOTALS_COLS = [
    "FechaInicio", "FechaFin", "Periodo", "Medio", "Canal", "GEstacion",
    "FHoraria", "NoCorte", "TipoSpot", "Genero", "TipoMaster", "IdMaster",
    "Version", "Anunciante", "Marca", "Submarca", "Producto", "Segmento",
    "Industria", "Mercado", "DTeorica", "TotalHit", "TotalInversion", "Testigo",
]

_TABLE_MAP: Dict[str, tuple] = {
    "tv":                    ("auditsa_api_tv",                    _TV_RADIO_COLS),
    "radio":                 ("auditsa_api_radio",                 _TV_RADIO_COLS),
    "impresos":              ("auditsa_api_impresos",              _IMPRESOS_COLS),
    "media_master_library":  ("auditsa_api_media_master_library",  _MEDIA_MASTER_COLS),
    "media_master_totals":   ("auditsa_api_media_master_totals",   _MEDIA_MASTER_TOTALS_COLS),
}


def _to_date(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str) and len(val) >= 10:
        try:
            return date(int(val[:4]), int(val[5:7]), int(val[8:10]))
        except (ValueError, IndexError):
            return None
    return None


def _sanitize(val: Any) -> Any:
    """Convierte tipos de pandas (pd.NA, pd.NaT, Int64, float NaN) a tipos Python nativos.
    pymssql no puede serializar tipos de pandas directamente."""
    if val is pd.NA or val is pd.NaT:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    # pandas nullable integers (Int8/16/32/64) → int o None
    if hasattr(val, "item"):
        try:
            return val.item()
        except (ValueError, AttributeError):
            return None
    return val


def write_to_sqlserver(records: List[Dict], media_type: str) -> int:
    """
    Inserta registros en mxTarifas.dbo.<tabla> vía SQL Server.
    Retorna el número de registros insertados.
    Lanza excepción si falla — el llamador decide si es fatal o no.
    """
    if not records:
        return 0

    table_name, columns = _TABLE_MAP[media_type]
    columns_sql = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    insert_sql = text(
        f"INSERT INTO [mxTarifas].[dbo].[{table_name}] ({columns_sql}) VALUES ({placeholders})"
    )

    clean: List[Dict] = []
    for r in records:
        row = {c: _sanitize(r.get(c)) for c in columns}
        if "Fecha" in row:
            row["Fecha"] = _to_date(row["Fecha"])
        clean.append(row)

    session = SqlServerTarifasSessionLocal()
    try:
        total = 0
        for i in range(0, len(clean), _BATCH_SIZE):
            session.execute(insert_sql, clean[i : i + _BATCH_SIZE])
            total += len(clean[i : i + _BATCH_SIZE])
        session.commit()
        logger.info("SQL Server mxTarifas.dbo.%s — %d registros insertados", table_name, total)
        return total
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
