"""
Caché en memoria para stats de las tablas grandes (TV, Radio, Impresos).
Las queries de stats sobre 20M filas tardan ~40s sin caché.
Con caché: primer load es background en startup, luego respuesta instantánea.
TTL configurable (defecto: 10 minutos).
"""

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

_TTL_SECONDS = 600  # 10 minutos

_cache: dict[str, dict] = {}


def get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    if time.time() - entry["ts"] > _TTL_SECONDS:
        del _cache[key]
        return None
    return entry["data"]


def set(key: str, data: Any) -> None:
    _cache[key] = {"data": data, "ts": time.time()}


def make_key(*parts) -> str:
    """Genera una clave de caché concatenando los parámetros significativos."""
    return ":".join(str(p) for p in parts if p is not None)


def invalidate(key: str) -> None:
    _cache.pop(key, None)


def invalidate_prefix(prefix: str) -> None:
    """Invalida todas las claves que comienzan con el prefijo dado."""
    keys = [k for k in _cache if k.startswith(prefix)]
    for k in keys:
        del _cache[k]


def invalidate_all() -> None:
    _cache.clear()
