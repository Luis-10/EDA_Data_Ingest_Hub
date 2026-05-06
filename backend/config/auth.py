from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import TYPE_CHECKING, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from config.settings import get_settings

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

"""
Autenticación y autorización.
En este archivo se definen las funciones y clases necesarias para la autenticación y autorización de usuarios.
"""

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


@lru_cache
def _get_users() -> dict:
    # Construye el store de usuarios una sola vez (lazy + cacheado).
    s = get_settings()
    return {
        s.admin_username: {
            "username": s.admin_username,
            "hashed_password": pwd_context.hash(s.admin_password),
            "role": "admin",
        },
        s.reader_username: {
            "username": s.reader_username,
            "hashed_password": pwd_context.hash(s.reader_password),
            "role": "reader",
        },
    }


def authenticate_user(username: str, password: str) -> Optional[dict]:
    # Autentica a un usuario y devuelve sus datos si es válido.
    users = _get_users()
    user = users.get(username)
    if not user or not pwd_context.verify(password, user["hashed_password"]):
        return None
    return user


def authenticate_user_from_db(username: str, password: str, db: "Session") -> Optional[dict]:
    # Autentica a un usuario registrado en la base de datos.
    from models.catalogues_user_role import db_catalogue_user, db_catalogue_roles

    db_user = db.query(db_catalogue_user).filter(db_catalogue_user.user == username).first()
    if not db_user:
        return None
    if not pwd_context.verify(password, db_user.password_hash):
        return None

    role_obj = db.query(db_catalogue_roles).filter(db_catalogue_roles.id == db_user.id_role).first()
    role_name = role_obj.role if role_obj else "standard"

    db_user.last_connection = datetime.now(timezone.utc)
    db.commit()

    return {
        "username": db_user.user,
        "hashed_password": db_user.password_hash,
        "role": role_name,
        "id_account": db_user.id_account,
    }


def create_access_token(data: dict) -> str:
    # Crea un token de acceso JWT con los datos proporcionados y una expiración.
    s = get_settings()
    expire = datetime.now(timezone.utc) + timedelta(minutes=s.jwt_expire_minutes)
    return jwt.encode(
        {**data, "exp": expire}, s.jwt_secret_key, algorithm=s.jwt_algorithm
    )


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    # Obtiene el usuario actual a partir de un token de acceso JWT.
    s = get_settings()
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token inválido o expirado",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, s.jwt_secret_key, algorithms=[s.jwt_algorithm])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        allowed_media: list = payload.get("allowed_media") or ["tv", "radio", "impresos"]
        if not username or not role:
            raise credentials_exc
    except JWTError:
        raise credentials_exc
    return {"username": username, "role": role, "allowed_media": allowed_media}


async def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    # Sólo usuarios con rol 'admin' pueden ejecutar ETL, DELETE y fix-nulls.
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requiere rol de administrador para esta operación",
        )
    return current_user


async def require_reader(current_user: dict = Depends(get_current_user)) -> dict:
    # Cualquier usuario autenticado puede leer datos.
    return current_user


def require_media_access(tipo: str):
    """Dependency factory — blocks access if the user's account doesn't cover 'tipo'."""
    async def _check(current_user: dict = Depends(get_current_user)) -> dict:
        if current_user["role"] == "admin":
            return current_user
        allowed = current_user.get("allowed_media") or ["tv", "radio", "impresos"]
        if tipo not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Tu cuenta no tiene acceso al medio '{tipo}'",
            )
        return current_user
    return _check
