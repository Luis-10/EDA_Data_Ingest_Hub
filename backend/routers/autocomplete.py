"""
Router de Autocomplete — sugerencias para filtros del dashboard con filtros en cascada.

Optimizaciones de rendimiento:
  - Caché en memoria con TTL de 60 s para evitar queries repetidas.
  - DISTINCT + LIMIT por tabla para reducir filas transferidas.
  - Early exit cuando ya se tienen suficientes resultados.

Lógica de cascada:
  - Cada tabla tiene su propio conjunto de columnas filtrables.
  - Si un filtro activo NO existe en una tabla → esa tabla se excluye del resultado.
"""

import hashlib
import time
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.database import get_db
from config.auth import require_reader
from models.api_auditsa_tv import db_auditsa_tv
from models.api_auditsa_radio import db_auditsa_radio
from models.api_auditsa_impresos import db_auditsa_impresos

router = APIRouter(prefix="/api/autocomplete", tags=["Autocomplete"])

# ── Caché en memoria con TTL ──
_cache: dict[str, tuple[float, list[str]]] = {}
_CACHE_TTL = 300  # segundos
_CACHE_MAX_SIZE = 2000


def _cache_get(key: str) -> list[str] | None:
    entry = _cache.get(key)
    if entry is None:
        return None
    ts, data = entry
    if time.monotonic() - ts > _CACHE_TTL:
        _cache.pop(key, None)
        return None
    return data


def _cache_set(key: str, data: list[str]):
    if len(_cache) >= _CACHE_MAX_SIZE:
        sorted_keys = sorted(_cache, key=lambda k: _cache[k][0])
        for k in sorted_keys[: _CACHE_MAX_SIZE // 4]:
            _cache.pop(k, None)
    _cache[key] = (time.monotonic(), data)


#  Columnas filtrables por tabla
_TV_FILTER_COLS = {
    "anunciante":       db_auditsa_tv.Anunciante,
    "marca":            db_auditsa_tv.Marca,
    "submarca":         db_auditsa_tv.Submarca,
    "medio":            db_auditsa_tv.Medio,
    "tipo_medio":       db_auditsa_tv.TipoMedio,
    "localidad":        db_auditsa_tv.Localidad,
    "industria":        db_auditsa_tv.Industria,
    "sector_industria": db_auditsa_tv.Industria,  # campo unificado → mapea a Industria en TV
}
_RADIO_FILTER_COLS = {
    "anunciante":        db_auditsa_radio.Anunciante,
    "marca":             db_auditsa_radio.Marca,
    "submarca":          db_auditsa_radio.Submarca,
    "medio":             db_auditsa_radio.Medio,
    "tipo_medio":        db_auditsa_radio.TipoMedio,
    "localidad":         db_auditsa_radio.Localidad,
    "sector_industria":  db_auditsa_radio.Industria,
    "industria":         db_auditsa_radio.Industria,
}
_IMP_FILTER_COLS = {
    "anunciante":        db_auditsa_impresos.Anunciante,
    "marca":             db_auditsa_impresos.Marca,
    "submarca":          db_auditsa_impresos.Submarca,
    "medio":             db_auditsa_impresos.Medio,
    "categoria":         db_auditsa_impresos.Categoria,
    "fuente":            db_auditsa_impresos.Fuente,
    "sector":            db_auditsa_impresos.Sector,
    "sector_industria":  db_auditsa_impresos.Sector,
}

_TV_FECHA    = db_auditsa_tv.Fecha
_RADIO_FECHA = db_auditsa_radio.Fecha
_IMP_FECHA   = db_auditsa_impresos.Fecha

FIELD_MAP = {
    "Anunciante": [
        (db_auditsa_tv.Anunciante,       _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Anunciante,    _RADIO_FILTER_COLS, _RADIO_FECHA),
        (db_auditsa_impresos.Anunciante, _IMP_FILTER_COLS,   _IMP_FECHA),
    ],
    "Marca": [
        (db_auditsa_tv.Marca,       _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Marca,    _RADIO_FILTER_COLS, _RADIO_FECHA),
        (db_auditsa_impresos.Marca, _IMP_FILTER_COLS,   _IMP_FECHA),
    ],
    "Categoria": [
        (db_auditsa_impresos.Categoria, _IMP_FILTER_COLS, _IMP_FECHA),
    ],
    "Localidad": [
        (db_auditsa_tv.Localidad,    _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Localidad, _RADIO_FILTER_COLS, _RADIO_FECHA),
    ],
    "TipoMedio": [
        (db_auditsa_tv.TipoMedio,    _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.TipoMedio, _RADIO_FILTER_COLS, _RADIO_FECHA),
    ],
    "Medio": [
        (db_auditsa_tv.Medio,       _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Medio,    _RADIO_FILTER_COLS, _RADIO_FECHA),
        (db_auditsa_impresos.Medio, _IMP_FILTER_COLS,   _IMP_FECHA),
    ],
    "Fuente": [
        (db_auditsa_impresos.Fuente, _IMP_FILTER_COLS, _IMP_FECHA),
    ],
    "Sector": [
        (db_auditsa_impresos.Sector, _IMP_FILTER_COLS, _IMP_FECHA),
    ],
    "SectorIndustria": [
        (db_auditsa_tv.Industria,    _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Industria,  _RADIO_FILTER_COLS, _RADIO_FECHA),
        (db_auditsa_impresos.Sector,  _IMP_FILTER_COLS,   _IMP_FECHA),
    ],
    "Industria": [
        (db_auditsa_tv.Industria,    _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Industria, _RADIO_FILTER_COLS, _RADIO_FECHA),
    ],
    "Submarca": [
        (db_auditsa_tv.Submarca,       _TV_FILTER_COLS,    _TV_FECHA),
        (db_auditsa_radio.Submarca,    _RADIO_FILTER_COLS, _RADIO_FECHA),
        (db_auditsa_impresos.Submarca, _IMP_FILTER_COLS,   _IMP_FECHA),
    ],
}

EXCLUDED = {"N.A.", "n.a.", "", None}


def _parse_csv(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


# Tablas con índice B-tree en Industria (requerido para loose index scan).
# Columnas de baja cardinalidad donde DISTINCT sobre millones de filas es lento.
_LOOSE_SCAN_INDUSTRIA_TABLES = [
    "auditsa_api_tv",
    "auditsa_api_radio",
]


def _loose_index_scan_industria(db: Session) -> list[str]:
    """
    Loose index scan vía recursive CTE para obtener todos los valores distintos de Industria
    en TV y Radio. ~14ms vs ~1760ms del DISTINCT tradicional sobre 15M filas.

    Ignora filtros de fecha y cascada porque la cardinalidad es tan baja (≤ 30 valores totales)
    que devolver el universo completo es aceptable para el UX del dropdown.
    """
    sql_template = """
    WITH RECURSIVE t AS (
      (SELECT "Industria" AS val FROM "{table}"
       WHERE "Industria" IS NOT NULL
         AND "Industria" <> 'N.A.'
         AND "Industria" <> ''
       ORDER BY "Industria" LIMIT 1)
      UNION ALL
      SELECT (SELECT "Industria" FROM "{table}"
              WHERE "Industria" > t.val
                AND "Industria" IS NOT NULL
                AND "Industria" <> 'N.A.'
                AND "Industria" <> ''
              ORDER BY "Industria" LIMIT 1)
      FROM t
      WHERE t.val IS NOT NULL
    )
    SELECT val FROM t WHERE val IS NOT NULL
    """
    results: set[str] = set()
    for table in _LOOSE_SCAN_INDUSTRIA_TABLES:
        rows = db.execute(text(sql_template.format(table=table))).fetchall()
        for (val,) in rows:
            if val and val.strip():
                results.add(val.strip())
    return sorted(results)


def _build_subquery(db: Session, target_col, table_filter_cols, fecha_col,
                    active_filters, fi, ff, q, per_table):
    """Construye una sub-query DISTINCT + LIMIT para una tabla."""
    sq = db.query(target_col.label("val")).filter(
        target_col.isnot(None),
        target_col != "N.A.",
        target_col != "",
    )

    for fkey, vals in active_filters.items():
        sq = sq.filter(table_filter_cols[fkey].in_(vals))

    if fi:
        sq = sq.filter(fecha_col >= fi)
    if ff:
        sq = sq.filter(fecha_col <= ff)

    if q:
        sq = sq.filter(target_col.ilike(f"%{q}%"))

    return sq.distinct().limit(per_table)


@router.get("", summary="Sugerencias de autocompletado con filtros en cascada")
async def autocomplete(
    field: str = Query(...),
    q: str = Query(default=""),
    limit: int = Query(default=10, ge=1, le=10000),
    anunciante: Optional[str] = Query(default=None),
    marca:      Optional[str] = Query(default=None),
    medio:      Optional[str] = Query(default=None),
    tipo_medio: Optional[str] = Query(default=None),
    localidad:  Optional[str] = Query(default=None),
    categoria:  Optional[str] = Query(default=None),
    fuente:           Optional[str] = Query(default=None),
    sector:           Optional[str] = Query(default=None),
    sector_industria: Optional[str] = Query(default=None),
    industria:        Optional[str] = Query(default=None),
    submarca:         Optional[str] = Query(default=None),
    fecha_inicio: Optional[str] = Query(default=None),
    fecha_fin:    Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: dict = Depends(require_reader),
):
    if field not in FIELD_MAP:
        return {"field": field, "suggestions": []}

    # ── Cache ──
    cache_key = hashlib.md5(
        f"{field}|{q}|{limit}|{anunciante}|{marca}|{medio}|{tipo_medio}|{localidad}"
        f"|{categoria}|{fuente}|{sector}|{sector_industria}|{industria}|{submarca}"
        f"|{fecha_inicio}|{fecha_fin}".encode()
    ).hexdigest()
    cached = _cache_get(cache_key)
    if cached is not None:
        return {"field": field, "suggestions": cached}

    # ── Filtros activos ──
    active_filters: dict[str, list[str]] = {}
    for key, val in [
        ("anunciante", anunciante), ("marca", marca),
        ("medio", medio), ("tipo_medio", tipo_medio),
        ("localidad", localidad), ("categoria", categoria),
        ("fuente", fuente), ("sector", sector),
        ("sector_industria", sector_industria), ("industria", industria),
        ("submarca", submarca),
    ]:
        parsed = _parse_csv(val)
        if parsed:
            active_filters[key] = parsed

    fi: date | None = None
    ff: date | None = None
    if fecha_inicio:
        try:
            fi = datetime.strptime(fecha_inicio.strip().replace("-", ""), "%Y%m%d").date()
        except ValueError:
            fi = None
    if fecha_fin:
        try:
            ff = datetime.strptime(fecha_fin.strip().replace("-", ""), "%Y%m%d").date()
        except ValueError:
            ff = None

    # ── Fast-path: Industria/SectorIndustria son baja cardinalidad.
    # Para q corto usamos loose index scan en TV+Radio y un DISTINCT directo en Impresos.Sector.
    if field in ("Industria", "SectorIndustria") and len(q) < 3:
        all_vals: set[str] = set(_loose_index_scan_industria(db))
        if field == "SectorIndustria":
            imp_rows = db.query(db_auditsa_impresos.Sector).filter(
                db_auditsa_impresos.Sector.isnot(None),
                db_auditsa_impresos.Sector.notin_(["N.A.", ""]),
            ).distinct().all()
            for (val,) in imp_rows:
                if val and val.strip():
                    all_vals.add(val.strip())
        all_vals_list = sorted(all_vals)
        if q:
            ql = q.lower()
            all_vals_list = [v for v in all_vals_list if ql in v.lower()]
        suggestions = all_vals_list[:limit]
        _cache_set(cache_key, suggestions)
        return {"field": field, "suggestions": suggestions}

    # ── Ejecutar queries por tabla y combinar resultados ──
    per_table = limit * 2
    results: set[str] = set()

    for (target_col, table_filter_cols, fecha_col) in FIELD_MAP[field]:
        skip = False
        for fkey in active_filters:
            if fkey not in table_filter_cols:
                skip = True
                break
            if table_filter_cols[fkey] is target_col:
                skip = True
                break
        if skip:
            continue

        rows = _build_subquery(
            db, target_col, table_filter_cols, fecha_col,
            active_filters, fi, ff, q, per_table,
        ).all()

        for (val,) in rows:
            if val and val.strip() and val.strip() not in EXCLUDED:
                results.add(val.strip())

        # Early exit si ya tenemos suficientes resultados
        if len(results) >= limit:
            break

    suggestions = sorted(results)[:limit]
    _cache_set(cache_key, suggestions)
    return {"field": field, "suggestions": suggestions}
