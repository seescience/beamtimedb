#!/usr/bin/env python
"""
function:  create_beamtimedb

Example creation:

from beamtimedb import create_beamtimedb
create_beamtimedb('beamtime', server='postgresql', create=True,
                  user='admin', password='secret',
                  host='N.X.aps.anl.gov', port=5432)

"""
import time
from datetime import datetime

from sqlalchemy import (create_engine, Text, Table, Column,
                        ForeignKey, Integer, Boolean, String, Text, DateTime)

from sqlalchemy_utils import database_exists, create_database

from .simpledb import SimpleDB
from .tables import (info, messages, user_types, user_level, esaf_type,
                     esaf_status, folder_status, insts,
                     funding, prop_fund, runs, beamlines, technique, acknow, users,
                     proposals, pvlog_template, experiments, queue, expt_user,
                     expt_staff, expt_tech, expt_fund, expt_acknows)

ENGINE = None

# some status values
ESAF_STATUS = ('Pending', 'Approved', 'Rejected', 'Conditional Approval')

ESAF_TYPES = ('APS Staff', 'Beamline set up', 'CAT Member', 'CAT Staff',
              'CNM Staff', 'GUP', 'Industrial GUP', 'No X-ray', 'PUP')

USER_TYPES = ('On-site', 'Remote', 'Mail-in',
              'Off-site/Co-proposer', 'Observer', 'Beamline Staff')

USER_LEVEL = ('Faculty/professional staff', 'Graduate student', 'Other',
              'Post-doc', 'Retired or self employed', 'Undergraduate student')

BEAMLINES = ('13-BM-C', '13-BM-D', '13-ID-C,D', '13-ID-E', '6-BM-A,B', '3-ID-B,C,D')

FOLDER_STATUS = ('unknown', 'requested', 'pending', 'created', 'cancelled', 'deleted')


def hasdb(dbname, create=False, server='postgresql',
             user='', password='', host='', port=5432):
    """
    return whether a database existsin the postgresql server,
    optionally creating (but leaving it empty) said database.
    """
    conn_str= f'{server}://{user}:{password}@{host}:{int(port)}/{dbname}'
    engine = create_engine(conn_str)
    if create and not database_exists(engine.url):
        create_database(engine.url)
    return database_exists(engine.url)

def IntCol(name, **kws):
    return Column(name, Integer, **kws)

def StrCol(name, size=None, **kws):
    val = Text
    if size is not None:
        val = String(size)
    return Column(name, val, **kws)

def PointerCol(name, other=None, keyid='id', **kws):
    if other is None:
        other = name
    return Column("%s_%s" % (name, keyid), None,
                  ForeignKey('%s.%s' % (other, keyid)), **kws)

def create_beamtimedb(dbname, server='postgresql', create=True,
                      user='', password='',  host='', port=5432, **kws):
    """Create a BeamtimeDB:

    arguments:
    ---------
    dbname    name of database

    options:
    --------
    server    type of database server (postgresql only at the moment)
    host      host serving database
    port      port number for database
    user      user name for database
    password  password for database
    """

    conn = {'user':user, 'password': password,
            'server': server, 'host': host, 'port':port}

    if hasdb(dbname, create=False, **conn):
        print("DB exists!")
        return

    if not hasdb(dbname, create=True, **conn):
        raise ValueError(f"could not create database '{dbname}'")

    db = SimpleDB(dbname, **conn)
    ENGINE = db.engine
    metadata = db.metadata

    metadata.create_all(bind=ENGINE)
    time.sleep(0.1)

    #db = SimpleDB(dbname, **conn)
    
    # add some initial data:
    for table, values in (('esaf_status', ESAF_STATUS),
                          ('folder_status', FOLDER_STATUS),
                          ('esaf_type', ESAF_TYPES),
                          ('user_type', USER_TYPES),
                          ('user_level', USER_LEVEL),
                          ('beamline', BEAMLINES),
                          ):
        for val in values:
            db.insert(table, name=val)

    for key, value in (("version", "1.1"),
                       ("version", "1.2"),                       
                       ):
        db.set_info(key, value)

    print(f"Created database for beamlinedb: '{dbname}'")
    return

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Create a BeamtimeDB")
    parser.add_argument('dbname', help='name of database to create')
    parser.add_argument('--host', default='', help='hostname (e.g. localhost)')
    parser.add_argument('--port', default=5432, type=int, help='port number for database')
    parser.add_argument('--user', default='', help='user name for database')
    parser.add_argument('--password', default='', help='password for database')
    args = parser.parse_args()
    create_beamtimedb(args.dbname, host=args.host, port=args.port, user=args.user, password=args.password)