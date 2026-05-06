"""
Router de Exportación — devuelve registros filtrados para generar Excel en el frontend.
Soporta TV, Radio e Impresos con selección dinámica de columnas.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import literal

from config.database import get_db
from config.auth import require_reader
from config.schemas import FechaQuery
from models.api_auditsa_tv import db_auditsa_tv
from models.api_auditsa_radio import db_auditsa_radio
from models.api_auditsa_impresos import db_auditsa_impresos
from services.session_log_service import build_sql, log_action

router = APIRouter(prefix="/api/export", tags=["Export"])
logger = logging.getLogger(__name__)

MAX_ROWS = 1_000_000  # Excel .xlsx admite hasta 1,048,576 filas por hoja

_MODELS = {
    "tv": db_auditsa_tv,
    "radio": db_auditsa_radio,
    "impresos": db_auditsa_impresos,
}


def _parse_csv(val: Optional[str]) -> list[str]:
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


@router.get("/{tipo}", summary="Obtener registros filtrados para exportación a Excel")
async def export_records(
    tipo: str,
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    background_tasks: BackgroundTasks,
    columns: str = Query(..., description="Columnas separadas por coma (ej: Fecha,Marca,Tarifa)"),
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    sector: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    fuente: Optional[str] = Query(default=None),
    limit: int = Query(default=MAX_ROWS, le=MAX_ROWS),
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_reader),
):
    """
    Devuelve registros filtrados para exportación a Excel.
    La columna especial 'Ins' se genera como literal 1 (cada fila = 1 inserción).
    Máximo 1,000,000 registros por consulta (límite práctico de una hoja .xlsx).
    """
    if tipo not in _MODELS:
        raise HTTPException(
            status_code=400,
            detail=f"Tipo inválido: {tipo}. Válidos: {list(_MODELS.keys())}",
        )

    model = _MODELS[tipo]
    requested_cols = _parse_csv(columns)
    if not requested_cols:
        raise HTTPException(status_code=400, detail="Debe especificar al menos una columna")

    # Columnas válidas del modelo (excluir 'id' autoincremental)
    valid_db_cols = {c.name for c in model.__table__.columns} - {"id"}

    # Construir lista de columnas SELECT
    select_cols = []
    col_labels = []
    for name in requested_cols:
        if name == "Ins":
            # Columna calculada: cada fila = 1 inserción
            select_cols.append(literal(1).label("Ins"))
            col_labels.append("Ins")
        elif name in valid_db_cols:
            select_cols.append(getattr(model, name))
            col_labels.append(name)
        else:
            logger.warning(f"Export: columna '{name}' no existe en modelo {tipo}, ignorada")

    if not select_cols:
        raise HTTPException(status_code=400, detail="Ninguna columna válida seleccionada")

    # Query base con rango de fechas
    fi = datetime.strptime(fecha_inicio, "%Y%m%d").date()
    ff = datetime.strptime(fecha_fin, "%Y%m%d").date()

    q = db.query(*select_cols).filter(model.Fecha >= fi, model.Fecha <= ff)

    # Filtros comunes (existen en las 3 tablas)
    for param_val, col_name in [
        (anunciante, "Anunciante"),
        (marca, "Marca"),
        (submarca, "Submarca"),
        (medio, "Medio"),
    ]:
        vals = _parse_csv(param_val)
        if vals and col_name in valid_db_cols:
            q = q.filter(getattr(model, col_name).in_(vals))

    # Filtros específicos por tipo
    if tipo in ("tv", "radio"):
        for param_val, col_name in [
            (tipo_medio, "TipoMedio"),
            (localidad, "Localidad"),
        ]:
            vals = _parse_csv(param_val)
            if vals and col_name in valid_db_cols:
                q = q.filter(getattr(model, col_name).in_(vals))
        # Industria aplica a TV y Radio (ambas tienen la columna Industria)
        vals = _parse_csv(industria)
        if vals and "Industria" in valid_db_cols:
            q = q.filter(model.Industria.in_(vals))
        if tipo == "radio":
            vals = _parse_csv(sector_industria)
            if vals and "Industria" in valid_db_cols:
                q = q.filter(model.Industria.in_(vals))
    elif tipo == "impresos":
        for param_val, col_name in [
            (categoria, "Categoria"),
            (sector, "Sector"),
            (fuente, "Fuente"),
        ]:
            vals = _parse_csv(param_val)
            if vals and col_name in valid_db_cols:
                q = q.filter(getattr(model, col_name).in_(vals))

    # Contar total antes del límite
    total_count = q.count()

    # Ejecutar con límite y orden
    rows = q.order_by(model.Fecha).limit(limit).all()

    # Serializar a lista de dicts
    records = []
    for row in rows:
        record = {}
        for i, label in enumerate(col_labels):
            val = row[i]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif val is None:
                val = ""
            record[label] = val
        records.append(record)

    logger.info(
        f"Export {tipo}: {len(records)} registros exportados "
        f"({total_count} disponibles) | cols={col_labels}"
    )

    source = f"public.auditsa_api_{tipo}"
    sql_cmd = build_sql(
        instruction="EXPORT",
        tipo=tipo,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        filters={
            "Anunciante": anunciante, "Marca": marca, "Submarca": submarca,
            "Medio": medio, "TipoMedio": tipo_medio, "Localidad": localidad,
            "Categoria": categoria, "Industria": industria,
            "Sector": sector or sector_industria, "Fuente": fuente,
        },
        columns=columns,
        total_rows=len(records),
    )
    background_tasks.add_task(log_action, current_user["username"], "EXPORT", sql_cmd, source)

    return {
        "tipo": tipo,
        "total_available": total_count,
        "total_exported": len(records),
        "truncated": total_count > limit,
        "columns": col_labels,
        "records": records,
    }
