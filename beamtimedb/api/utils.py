# Contains shared utilities for the beamtimedb API.
from typing import Any
from collections.abc import Iterator
from sqlalchemy import create_engine, Table, select
from sqlalchemy.engine import Connection
from fastapi import HTTPException, status

from beamtimedb.config.config import Settings

# Import environment variables
settings = Settings()

# Create engine
engine = create_engine(settings.database_uri.get_secret_value())

# Session generator
def get_conn() -> Iterator[Connection]:
    with engine.begin() as conn:
        yield conn

# Return item in table matching item_id or give error code 404 if not found
def row_or_404(conn: Connection, table: Table, item_id: int) -> dict[str, Any]:
    row = conn.execute(select(table).where(table.c.id == item_id)).mappings().first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"{table.name} {item_id} not found")
    return dict(row)