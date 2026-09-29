"""One place to turn the SQLAlchemy-style DATABASE_URL into a libpq DSN.

SQLAlchemy needs the driver in the scheme (`postgresql+psycopg://...`) but psycopg
rejects it, so the two consumers of DATABASE_URL cannot share the raw string.

The variable is read when the helper is called rather than at import, so the
order of `load_dotenv()` relative to the imports does not matter.
"""

import os
from urllib.parse import urlsplit, urlunsplit

_DRIVER_SCHEMES = ("postgresql+psycopg://", "postgresql+psycopg2://",
                   "postgres+psycopg://", "postgres+psycopg2://")


def database_url() -> str:
    return os.environ.get("DATABASE_URL") or ""


def psycopg_dsn(url: str = None) -> str:
    """Strip any SQLAlchemy driver suffix so psycopg can parse the URL."""
    url = database_url() if url is None else url
    for scheme in _DRIVER_SCHEMES:
        if url.startswith(scheme):
            return urlunsplit(urlsplit(url.replace(scheme, "postgresql://", 1)))
    return url
