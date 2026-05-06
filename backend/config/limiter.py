from slowapi import Limiter
from slowapi.util import get_remote_address

"""
Límite de tasa de solicitudes.
En este archivo se define el límite de tasa de solicitudes para la aplicación.
"""

# Límite global de 200 req/min por IP; cada endpoint puede sobrescribirlo.
limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])
