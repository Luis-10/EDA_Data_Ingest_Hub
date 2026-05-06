import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_TABLE_MAP = {
    "tv":       "public.auditsa_api_tv",
    "radio":    "public.auditsa_api_radio",
    "impresos": "public.auditsa_api_impresos",
}


def build_sql(
    instruction: str,
    tipo: str,
    fecha_inicio: str,
    fecha_fin: str,
    filters: dict[str, Optional[str]],
    columns: Optional[str] = None,
    total_rows: Optional[int] = None,
) -> str:
    """Construye una representación legible del comando ejecutado."""
    table = _TABLE_MAP.get(tipo, tipo)
    fi = f"{fecha_inicio[:4]}-{fecha_inicio[4:6]}-{fecha_inicio[6:]}"
    ff = f"{fecha_fin[:4]}-{fecha_fin[4:6]}-{fecha_fin[6:]}"

    col_clause = columns if columns else "*"
    parts = [f"{instruction} {col_clause} FROM {table}"]

    where = [f"Fecha BETWEEN '{fi}' AND '{ff}'"]
    for col, raw_val in filters.items():
        if not raw_val:
            continue
        vals = [v.strip() for v in raw_val.split(",") if v.strip()]
        if not vals:
            continue
        preview = ", ".join(f"'{v}'" for v in vals[:3])
        if len(vals) > 3:
            preview += f", ... ({len(vals)} total)"
        where.append(f"{col} IN ({preview})")

    parts.append("WHERE " + " AND ".join(where))
    if total_rows is not None:
        parts.append(f"-- {total_rows} rows")
    return " ".join(parts)


def log_action(
    username: str,
    instruction: str,
    sql_command: str,
    source: str,
) -> None:
    """
    Escribe una entrada de log en PostgreSQL y SQL Server appSAC.
    Diseñado para ejecutarse como BackgroundTask — crea sus propias sesiones
    y tiene graceful degradation en ambas bases.
    """
    from config.database import SessionLocal, SqlServerSacSessionLocal
    from models.catalogues_user_role import db_catalogue_user, db_session_logs
    from models.sqlserver_users import ss_catalogue_session_logs

    now = datetime.now(timezone.utc)
    id_user = 0

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    pg_db = SessionLocal()
    try:
        user_row = (
            pg_db.query(db_catalogue_user.id)
            .filter(db_catalogue_user.user == username)
            .first()
        )
        id_user = user_row[0] if user_row else 0

        pg_db.add(db_session_logs(
            id_user=id_user,
            datetime=now,
            instruction=instruction,
            sql_command=sql_command,
            source=source,
        ))
        pg_db.commit()
    except Exception as exc:
        pg_db.rollback()
        logger.warning("session_log PG write error [%s]: %s", username, exc)
    finally:
        pg_db.close()

    # ── SQL Server appSAC ─────────────────────────────────────────────────────
    try:
        ss_db = SqlServerSacSessionLocal()
        try:
            ss_db.add(ss_catalogue_session_logs(
                id_user=id_user,
                datetime=now,
                instruction=instruction,
                sql_command=sql_command,
                source=source,
            ))
            ss_db.commit()
        except Exception as exc:
            ss_db.rollback()
            logger.warning("session_log SS write error [%s]: %s", username, exc)
        finally:
            ss_db.close()
    except Exception as exc:
        logger.warning("session_log SS connection error: %s", exc)
