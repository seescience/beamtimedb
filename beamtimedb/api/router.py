# Defines generic endpoints for beamtimedb and utils supporting their construction.

from typing import Optional
from functools import cache

from pydantic import TypeAdapter, ValidationError, BaseModel, create_model, ConfigDict
from fastapi import APIRouter, HTTPException, Query, Request, Response, status, Depends
from sqlalchemy import Table, func, select, Column
from sqlalchemy.engine import Connection
from sqlalchemy.types import NullType

from beamtimedb.api.utils import get_conn, row_or_404

RESERVED_PARAMS = {"skip", "limit", "order_by"}

def _coerce(table: Table, key: str, raw: str):
    if key not in table.c:
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"unknown filter field '{key}' for {table.name}")
    try:
        return TypeAdapter(python_type(table.c[key])).validate_python(raw)
    except ValidationError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"invalid value '{raw}' for field '{key}'")


def _camel(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


def python_type(col: Column) -> type:
    """Python type for a column; foreign keys declared with no type
    take the type of the column they point to."""
    if isinstance(col.type, NullType):
        if col.foreign_keys:
            return python_type(next(iter(col.foreign_keys)).column)
        return str
    try:
        return col.type.python_type
    except NotImplementedError:
        return str


class ReadBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

# Creates Pydantic model dynamically from table by converting each column to a name and type.
@cache
def create_read_model(table: Table) -> BaseModel:
    model = {}
    for col in table.columns:
        model[col.name] = (Optional[python_type(col)], None)

    name = _camel(table.name)

    return create_model(f"{name}Read", __base__=ReadBase, **model)


# Router containing only GET endpoints.
def read_only_router(table: Table, prefix: str, tag: str) -> APIRouter:
    Read = create_read_model(table)
    router = APIRouter(prefix=prefix, tags=[tag])
    label = table.name

    @router.get("", response_model=list[Read], name=f"list_{label}",
                summary=f"List {label} records",
                description="Any column can be used as an equality filter, "
                            "e.g. `?beamline_id=2`. Use `order_by=-name` for "
                            "descending order. The total number of matching "
                            "rows is returned in the X-Total-Count header.")
    def list_items(request: Request, response: Response,
                   skip: int = Query(0, ge=0),
                   limit: int = Query(100, ge=1, le=1000),
                   order_by: str = "id",
                   conn: Connection = Depends(get_conn)):
        stmt = select(table)
        for key, raw in request.query_params.items():
            if key not in RESERVED_PARAMS:
                value = _coerce(table, key, raw)   # 400 on unknown field / bad value
                stmt = stmt.where(table.c[key] == value)

        total = conn.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
        response.headers["X-Total-Count"] = str(total)

        field = order_by.lstrip("-")
        if field not in table.c:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                f"cannot order by unknown field '{field}'")
        col = table.c[field]
        stmt = stmt.order_by(col.desc() if order_by.startswith("-") else col)
        rows = conn.execute(stmt.offset(skip).limit(limit)).mappings()
        return [dict(r) for r in rows]

    @router.get("/{item_id}", response_model=Read, name=f"get_{label}",
                summary=f"Get one {label}")
    def get_item(item_id: int, conn: Connection = Depends(get_conn)):
        return row_or_404(conn, table, item_id)

    return router