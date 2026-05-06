"""
Utilidad de normalización de strings para el proceso ETL.

Funciones para:
  - Quitar acentos de vocales (á→A, é→E, etc.)
  - Eliminar caracteres especiales/raros
  - Convertir a MAYÚSCULAS
  - Aplicable a DataFrames de Pandas y series individuales
"""

import re
import unicodedata

# Tabla de traducción rápida para acentos comunes en español
_ACCENT_TABLE = str.maketrans(
    "áéíóúàèìòùâêîôûäëïöüãõñÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÄËÏÖÜÃÕÑ",
    "AEIOUAEIOUAEIOUAEIOUAONAEIOUAEIOUAEIOUAEIOUAON",
)


def normalize_str(value: str) -> str:
    """
    Normaliza un string individual:
    1. Descompone caracteres Unicode (NFD) para separar acentos
    2. Elimina marcas diacríticas (acentos, tildes, diéresis)
    3. Convierte a MAYÚSCULAS
    4. Elimina caracteres no imprimibles / de control
    5. Preserva letras, números, espacios y puntuación básica
    """
    if not value or not isinstance(value, str):
        return value

    # Paso 1: Normalización Unicode NFD + eliminar diacríticos
    nfkd = unicodedata.normalize("NFKD", value)
    without_accents = "".join(
        ch for ch in nfkd if unicodedata.category(ch) != "Mn"
    )

    # Paso 2: Mayúsculas
    upper = without_accents.upper()

    # Paso 3: Eliminar caracteres de control y no imprimibles (preservar puntuación básica)
    cleaned = re.sub(r"[^\x20-\x7E\n]", "", upper)

    return cleaned


def normalize_series(series):
    """
    Aplica normalización a una Serie de Pandas.
    Preserva valores especiales como 'N.A.', NaN, None.
    """
    import pandas as pd

    def _safe_normalize(val):
        if pd.isna(val) or val in ("N.A.", "nan", "None", ""):
            return val
        return normalize_str(str(val))

    return series.map(_safe_normalize)


def normalize_dataframe_columns(df, columns: list):
    """
    Normaliza las columnas de string especificadas en un DataFrame de Pandas.
    Se aplica DESPUÉS del strip y ANTES del fillna/truncado.

    Args:
        df: DataFrame de Pandas
        columns: Lista de nombres de columnas a normalizar
    Returns:
        DataFrame con las columnas normalizadas
    """
    for col in columns:
        if col in df.columns:
            df[col] = normalize_series(df[col])
    return df
