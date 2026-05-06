import re
from typing import Annotated
from fastapi import Query, HTTPException
from pydantic import BaseModel, field_validator

"""
Esquemas de datos.
En este archivo se definen los esquemas de datos utilizados en la aplicación.
"""

# Expresión regular para validar formato de fecha YYYYMMDD.
FECHA_RE = re.compile(r"^\d{8}$")


def _validate_fecha(v: str) -> str:
    # Valida que la fecha tenga el formato YYYYMMDD.
    if not FECHA_RE.match(v):
        raise ValueError(
            f"Fecha '{v}' inválida: use formato YYYYMMDD (8 dígitos, ej: 20250703)"
        )
    return v


"""
Esquema de solicitud para ETL.
En este apartado se define el esquema de solicitud para la ETL.
"""


class ETLRequest(BaseModel):
    # Solicitud para ETL en una fecha específica.
    fecha: str  # Formato YYYYMMDD

    # @field_validator: Valida el campo "fecha" con _validate_fecha.
    @field_validator("fecha")
    @classmethod  # @classmethod: Método de clase para validar campo "fecha".
    def validate_fecha(cls, v: str) -> str:
        return _validate_fecha(v)


def fecha_query(description: str = "Fecha en formato YYYYMMDD") -> str:
    """Dependencia FastAPI para validar fecha en query params."""
    def _validate(v: str = Query(..., description=description, example="20250703")):
        if not FECHA_RE.match(v):
            raise HTTPException(
                status_code=422,
                detail=f"Fecha '{v}' inválida: use formato YYYYMMDD (ej: 20250703)"
            )
        return v
    return _validate


# Tipo anotado para query params opcionales de fecha
FechaQuery = Annotated[str, Query(pattern=r"^\d{8}$", description="Formato YYYYMMDD", example="20250703")]
FechaQueryOpt = Annotated[str | None, Query(pattern=r"^\d{8}$", description="Formato YYYYMMDD", example="20250703")]


class ETLRangeRequest(BaseModel):
    # Solicitud para ETL en rango de fechas.
    fecha_inicio: str
    fecha_fin: str

    # @field_validator: Valida los campos "fecha_inicio" y "fecha_fin" con _validate_fecha.
    @field_validator("fecha_inicio", "fecha_fin")
    @classmethod  # @classmethod: Método de clase para validar campos "fecha_inicio" y "fecha_fin".
    def validate_fechas(cls, v: str) -> str:
        return _validate_fecha(v)
