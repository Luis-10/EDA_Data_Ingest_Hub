import logging
from typing import Optional

logger = logging.getLogger(__name__)

_ALL_MEDIA = ["tv", "radio", "impresos"]

_SECTOR_MAP: dict[str, str] = {
    "television": "tv",
    "televisión": "tv",
    "tv": "tv",
    "radio": "radio",
    "impresos": "impresos",
    "impreso": "impresos",
    "prensa": "impresos",
    "print": "impresos",
    "periódicos": "impresos",
    "periodicos": "impresos",
    "revistas": "impresos",
    "revista": "impresos",
}


def _map_sector_name(name: str) -> Optional[str]:
    n = name.lower().strip()
    if n in _SECTOR_MAP:
        return _SECTOR_MAP[n]
    for key, media in _SECTOR_MAP.items():
        if key in n:
            return media
    return None


def get_allowed_media(id_account: Optional[int], sac_db) -> list[str]:
    """
    Returns the list of allowed media types for the given account.
    Runs a cross-database query against appSAC.relations.sectors_accounts
    joined with mxRepository.catalogues_src.sectors on the same SQL Server instance.
    Falls back to all media on any failure or when id_account is None.
    """
    if id_account is None:
        return _ALL_MEDIA[:]

    try:
        from sqlalchemy import text

        sql = text(
            """
            SELECT s.name
            FROM appSAC.relations.sectors_accounts sa
            INNER JOIN mxRepository.catalogues_src.sectors s
                ON s.id = sa.id_sector_src
            WHERE sa.id_account = :id_account
            """
        )
        rows = sac_db.execute(sql, {"id_account": id_account}).fetchall()

        if not rows:
            logger.warning(
                "No sectors found for id_account=%s — granting all media", id_account
            )
            return _ALL_MEDIA[:]

        media_set: set[str] = set()
        for row in rows:
            mapped = _map_sector_name(str(row[0] or ""))
            if mapped:
                media_set.add(mapped)

        if not media_set:
            logger.warning(
                "Sector names for id_account=%s could not be mapped — granting all media",
                id_account,
            )
            return _ALL_MEDIA[:]

        return sorted(media_set)

    except Exception as exc:
        logger.warning(
            "Failed to fetch sectors for id_account=%s: %s — granting all media",
            id_account,
            exc,
        )
        return _ALL_MEDIA[:]
