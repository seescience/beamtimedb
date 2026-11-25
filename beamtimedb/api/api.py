# Main entry point for the beamtimedb API.

from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.engine import Connection

from beamtimedb.api.router import read_only_router
from beamtimedb.api.utils import get_conn, settings
from beamtimedb.schema import create_beamtimedb
from beamtimedb import tables as T

# API on-start function. Creates the DB if it doesn't already exist using previously existing script in schema.py.
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_beamtimedb(settings.db_name, user=settings.db_uname.get_secret_value(), password=settings.db_pwd.get_secret_value())
    yield

app = FastAPI(lifespan=lifespan)

# Data structure containing all tables to create generic endpoints for.
# Assigns api prefixes and tags for each table.
tables = (
    (T.user_types,     "/user-types",       "static"),
    (T.user_level,     "/user-levels",      "static"),
    (T.esaf_type,      "/esaf-types",       "static"),
    (T.esaf_status,    "/esaf-statuses",    "static"),
    (T.folder_status,  "/folder-statuses",  "static"),
    (T.runs,           "/runs",             "experiments"),
    (T.beamlines,      "/beamlines",        "static"),
    (T.insts,          "/institutions",     "people"),
    (T.users,          "/people",           "people"),
    (T.funding,        "/funding",          "experiments"),
    (T.acknow,         "/acknowledgments",  "experiments"),
    (T.technique,      "/techniques",       "experiments"),
    (T.pvlog_template, "/pvlog-templates",  "experiments"),
    (T.proposals,      "/proposals",        "experiments"),
    (T.experiments,    "/experiments",      "experiments"),
    (T.queue,          "/queue",            "queue"),
    (T.messages,       "/messages",         "messages"),
)

# Creates generic endpoints for all tables specified above.
# The current generic endpoints are "get all" and "get by id".
for table, prefix, tag in tables:
    app.include_router(read_only_router(table, prefix, tag))

# I think this will be useful in production, but it's not being used yet.
@app.get("/health", tags=["meta"], summary="Check database connectivity")
def health(conn: Connection = Depends(get_conn)):
    conn.execute(text("SELECT 1"))
    return {"status": "ok"}