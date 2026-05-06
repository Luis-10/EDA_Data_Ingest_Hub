"""
Router de Registros — devuelve registros paginados para las tablas de detalle del dashboard.
Excluye columnas de tipo Id* automáticamente.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from config.database import get_db
from config.auth import require_reader
from config.schemas import FechaQuery
from models.api_auditsa_tv import db_auditsa_tv
from models.api_auditsa_radio import db_auditsa_radio
from models.api_auditsa_impresos import db_auditsa_impresos
from services.session_log_service import build_sql, log_action

router = APIRouter(prefix="/api/records", tags=["Records"])
logger = logging.getLogger(__name__)

_MODELS = {
    "tv": db_auditsa_tv,
    "radio": db_auditsa_radio,
    "impresos": db_auditsa_impresos,
}


def _display_columns(model) -> list[str]:
    """Retorna columnas excluyendo 'id' y las que comienzan con 'Id'."""
    return [
        c.name for c in model.__table__.columns
        if c.name != "id" and not c.name.startswith("Id")
    ]


def _parse_csv(val: Optional[str]) -> list[str]:
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


@router.get("/{tipo}", summary="Registros paginados de TV, Radio o Impresos")
async def get_records(
    tipo: str,
    fecha_inicio: FechaQuery,
    fecha_fin: FechaQuery,
    background_tasks: BackgroundTasks,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=10, le=200),
    anunciante: Optional[str] = Query(default=None),
    marca: Optional[str] = Query(default=None),
    submarca: Optional[str] = Query(default=None),
    medio: Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad: Optional[str] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_reader),
):
    if tipo not in _MODELS:
        return {"records": [], "columns": [], "total": 0, "page": page, "page_size": page_size, "pages": 0}

    if current_user["role"] != "admin":
        allowed = current_user.get("allowed_media") or ["tv", "radio", "impresos"]
        if tipo not in allowed:
            raise HTTPException(status_code=403, detail=f"Tu cuenta no tiene acceso al medio '{tipo}'")

    model = _MODELS[tipo]
    cols = _display_columns(model)
    select_cols = [getattr(model, c) for c in cols]

    fi = datetime.strptime(fecha_inicio, "%Y%m%d").date()
    ff = datetime.strptime(fecha_fin, "%Y%m%d").date()

    q = db.query(*select_cols).filter(model.Fecha >= fi, model.Fecha <= ff)

    # Filtros comunes
    valid = {c.name for c in model.__table__.columns}
    for param_val, col_name in [
        (anunciante, "Anunciante"),
        (marca, "Marca"),
        (submarca, "Submarca"),
        (medio, "Medio"),
    ]:
        vals = _parse_csv(param_val)
        if vals and col_name in valid:
            q = q.filter(getattr(model, col_name).in_(vals))

    # Filtros TV/Radio
    if tipo in ("tv", "radio"):
        for param_val, col_name in [
            (tipo_medio, "TipoMedio"),
            (localidad, "Localidad"),
        ]:
            vals = _parse_csv(param_val)
            if vals and col_name in valid:
                q = q.filter(getattr(model, col_name).in_(vals))
        vals = _parse_csv(industria)
        if vals and "Industria" in valid:
            q = q.filter(model.Industria.in_(vals))
        if tipo == "radio":
            vals = _parse_csv(sector_industria)
            if vals and "Industria" in valid:
                q = q.filter(model.Industria.in_(vals))
    elif tipo == "impresos":
        for param_val, col_name in [
            (categoria, "Categoria"),
            (sector_industria, "Sector"),
        ]:
            vals = _parse_csv(param_val)
            if vals and col_name in valid:
                q = q.filter(getattr(model, col_name).in_(vals))

    offset = (page - 1) * page_size
    count_col = func.count().over().label("_total")
    raw_rows = q.add_columns(count_col).order_by(model.Fecha).offset(offset).limit(page_size).all()

    total = raw_rows[0][-1] if raw_rows else 0
    pages = (total + page_size - 1) // page_size if total > 0 else 0

    records = []
    for row in raw_rows:
        record = {}
        for i, col_name in enumerate(cols):
            val = row[i]
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif val is None:
                val = ""
            record[col_name] = val
        records.append(record)

    source = f"public.auditsa_api_{tipo}"
    sql_cmd = build_sql(
        instruction="SELECT",
        tipo=tipo,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        filters={
            "Anunciante": anunciante, "Marca": marca, "Submarca": submarca,
            "Medio": medio, "TipoMedio": tipo_medio, "Localidad": localidad,
            "Categoria": categoria, "Industria": industria,
            "Sector": sector_industria,
        },
        total_rows=total,
    )
    background_tasks.add_task(log_action, current_user["username"], "SELECT", sql_cmd, source)

    return {
        "tipo": tipo,
        "columns": cols,
        "records": records,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
    }
