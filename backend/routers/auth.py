import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy.orm import Session

from config.auth import (
    authenticate_user,
    authenticate_user_from_db,
    create_access_token,
    get_current_user,
    pwd_context,
)
from config.database import get_db, get_sqlserver_sac_db
from services.account_sectors_service import get_allowed_media

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth"])


class RegisterRequest(BaseModel):
    username: str
    full_name: str
    email: str
    password: str
    account: str


# ── Accounts ──────────────────────────────────────────────────────────────────

@router.get("/accounts", summary="Lista de cuentas disponibles para registro")
async def get_accounts(ss_db: Session = Depends(get_sqlserver_sac_db)):
    """
    Retorna los nombres de cuenta desde appSAC.catalogue.accounts.
    Se usa para poblar el dropdown de selección en el registro de usuarios.
    """
    from models.sqlserver_users import ss_catalogue_accounts
    try:
        rows = ss_db.query(ss_catalogue_accounts.name).order_by(ss_catalogue_accounts.name).all()
        return {"accounts": [r[0] for r in rows]}
    except Exception as exc:
        logger.warning("No se pudo obtener cuentas desde SQL Server: %s", exc)
        return {"accounts": []}


# ── Login ─────────────────────────────────────────────────────────────────────

@router.post("/token", summary="Obtener token JWT de acceso")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
    ss_db: Session = Depends(get_sqlserver_sac_db),
):
    """
    Envía `username` y `password` para recibir un Bearer token JWT.

    Roles disponibles:
    - **admin** — puede ejecutar ETL, eliminar datos y normalizar nulls
    - **reader** — sólo lectura (stats, top-marcas, autocomplete)

    Verifica primero usuarios de entorno (.env) y luego usuarios registrados en DB.
    """
    # 1. Primero intenta con usuarios de entorno (admin / reader)
    user = authenticate_user(form_data.username, form_data.password)

    # 2. Si no se encontró, busca en la base de datos
    if not user:
        user = authenticate_user_from_db(form_data.username, form_data.password, db)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    id_account = user.get("id_account")
    allowed_media = get_allowed_media(id_account, ss_db)

    token = create_access_token({
        "sub": user["username"],
        "role": user["role"],
        "id_account": id_account,
        "allowed_media": allowed_media,
    })
    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user["role"],
        "username": user["username"],
        "allowed_media": allowed_media,
    }


# ── Register ───────────────────────────────────────────────────────────────────

@router.post("/register", summary="Registrar nuevo usuario", status_code=201)
async def register(
    payload: RegisterRequest,
    db: Session = Depends(get_db),
    ss_db: Session = Depends(get_sqlserver_sac_db),
):
    """
    Crea una nueva cuenta con rol **standard**.
    El administrador puede cambiar el rol posteriormente.
    El usuario queda registrado en PostgreSQL y en SQL Server (dual-write).
    """
    from models.catalogues_user_role import db_catalogue_roles, db_catalogue_user
    from models.sqlserver_users import ss_catalogue_roles, ss_catalogue_users, ss_catalogue_accounts

    # Validaciones de unicidad en PostgreSQL
    if db.query(db_catalogue_user).filter(db_catalogue_user.user == payload.username).first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El nombre de usuario ya está en uso",
        )
    if db.query(db_catalogue_user).filter(db_catalogue_user.email == payload.email).first():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está registrado",
        )

    now = datetime.now(timezone.utc)
    password_hash = pwd_context.hash(payload.password)

    # Resolver id_account desde appSAC.catalogue.accounts (fuente de verdad)
    id_account: int | None = None
    try:
        acc_row = (
            ss_db.query(ss_catalogue_accounts.id)
            .filter(ss_catalogue_accounts.name == payload.account)
            .first()
        )
        id_account = acc_row[0] if acc_row else None
    except Exception as exc:
        logger.warning("No se pudo resolver id_account para '%s': %s", payload.account, exc)

    # Obtener o crear el rol 'standard' en PostgreSQL
    std_role = db.query(db_catalogue_roles).filter(db_catalogue_roles.role == "standard").first()
    if not std_role:
        std_role = db_catalogue_roles(role="standard", status=True)
        db.add(std_role)
        db.flush()

    # Crear usuario en PostgreSQL
    new_user = db_catalogue_user(
        user=payload.username,
        full_name=payload.full_name,
        email=payload.email,
        password_hash=password_hash,
        create_date_time=now,
        id_role=std_role.id,
        account=payload.account,
        id_account=id_account,
        status=True,
    )
    db.add(new_user)
    db.commit()

    # Dual-write a SQL Server appSAC (graceful degradation si SS no está disponible)
    try:
        ss_std_role = ss_db.query(ss_catalogue_roles).filter(ss_catalogue_roles.role == "standard").first()
        if not ss_std_role:
            ss_std_role = ss_catalogue_roles(role="standard", status=True)
            ss_db.add(ss_std_role)
            ss_db.flush()

        ss_new_user = ss_catalogue_users(
            user=payload.username,
            full_name=payload.full_name,
            email=payload.email,
            password_hash=password_hash,
            create_date_time=now,
            id_role=ss_std_role.id,
            account=payload.account,
            id_account=id_account,
            status=True,
        )
        ss_db.add(ss_new_user)
        ss_db.commit()
    except Exception as exc:
        ss_db.rollback()
        logger.warning("SQL Server dual-write falló para registro de usuario '%s': %s", payload.username, exc)

    return {"message": "Usuario registrado exitosamente", "username": payload.username}


# ── Me ─────────────────────────────────────────────────────────────────────────

@router.get("/me", summary="Datos del usuario autenticado")
async def me(current_user: dict = Depends(get_current_user)):
    """Retorna el username y rol del usuario que tiene el token activo."""
    return current_user
