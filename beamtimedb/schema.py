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

from sqlalchemy import (MetaData, create_engine, text, Table, Column,
                        ForeignKey, Integer, String, Text, DateTime)

from sqlalchemy_utils import database_exists, create_database

from .simpledb import SimpleDB

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

PROCESS_STATUS = ('new', 'processed', 'modified', 'completed', 'locked')

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

    print("Conn ", conn)
    if hasdb(dbname, create=False, **conn):
        print("DB exists!")
        return

    if not hasdb(dbname, create=True, **conn):
        raise ValueError(f"could not create database '{dbname}'")

    db = SimpleDB(dbname, **conn)
    engine = db.engine
    metadata = db.metadata

    info = Table('info', metadata,
                 Column('key', Text, primary_key=True, unique=True),
                 StrCol('value'),
                 StrCol('notes'),
                 Column('modify_time', DateTime, default=datetime.now),
                 Column('create_time', DateTime, default=datetime.now),
                 IntCol('display_order')           )

    messages = Table('message', metadata,
                 Column('id', Integer, primary_key=True),
                 StrCol('text'),
                 Column('modify_time', DateTime, default=datetime.now))

    user_types = Table('user_type', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(64)))
    
    user_level = Table('user_level', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(64)))                       

    esaf_type = Table('esaf_type', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(64)))                       
                      
    esaf_status = Table('esaf_status', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(64)))                       

    folder_status = Table('esaf_status', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(64)))                       
    
    insts = Table('institution', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('name', String(2048)),
                  Column('city', String(512)),
                  Column('country', String(512)) )

    funding = Table('funding', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('agency',      String(512)),
                  Column('division',    String(512)),
                  Column('grant_number', String(512)) )
    

    runs = Table('run', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('name', String(64)))                       

    beamlines = Table('beamline', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('name', String(64)))                       

    technique = Table('technique', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(512)),
                      Column('user_name', String(64)),
                      Column('base_dir', String(512)),                      
                      Column('pvlog_template', text),
                      PointerColumn('beamline'),
                      )

    acknow = Table('acknowledgment', metadata,
                      Column('id', Integer, primary_key=True),
                      StrCol('title'),
                      StrCol('text'))
    

    users = Table('person', metadata,
                  Column('id', Integer, primary_key=True),                  
                  Column('badge', Integer),
                  StrCol('first_name'),
                  StrCol('last_name'),
                  StrCol('email'),
                  Column('orcid', String(64)),
                  PointerCol('affiliation', other='institution'),
                  PointerCol('user_level')  )

    
    proposals = Table('proposal', metadata,
                      Column('id', Integer, primary_key=True),
                      StrCol('title'),
                      PointerCol('spokesperson', other='person'))

    pvlog_template = Table('pvlog_template', metadata,
                      Column('id', Integer, primary_key=True),
                      PointerCol('beamline'),
                      StrCol('name'),
                      StrCol('value'))

    queue = Table('queue', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('experiment_number', Integer),
                      Column('proposal_number', Integer),
                      StrCol('title'),
                      StrCol('acknowledgements'),
                      StrCol('data_path'),
                      StrCol('pvlog_path'),
                      Column('doi', Integer))

    experiments = Table('experiment', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('time_request', Integer),                        
                        PointerCol('run'),
                        PointerCol('esaf_type'),
                        PointerCol('esaf_status'),
                        PointerCol('folder_status'),
                        PointerCol('process_status'),
                        PointerCol('technique'),
                        PointerCol('beamline'),
                        PointerCol('proposal'),
                        PointerCol('spokesperson', other='person'),
                        PointerCol('beamline_contact', other='person'),
                        StrCol('title'),
                        StrCol('description'),
                        Column('start_date', DateTime),
                        Column('end_date', DateTime),
                        Column('folder_create_time', DateTime),
                        StrCol('user_folder'),
                        StrCol('data_doi'),
                        StrCol('esaf_pdf_file'),
                        StrCol('proposal_pdf_file'),
                        Column('needs_doi', Integer),
                        Column('needs_folder', Integer),
                        Column('needs_pvlog', Integer),
                        StrCol('pvlog_file'),
                        )
    
    # join tables for many-to-one relations
    expt_user = Table('experiment_person', metadata,
                      PointerCol('experiment'),
                      PointerCol('person'),
                      PointerCol('user_type'))

    expt_tech = Table('experiment_technique', metadata,
                      PointerCol('experiment'),
                      PointerCol('technique'))

    expt_fund = Table('experiment_funding', metadata,
                      PointerCol('experiment'),
                      PointerCol('funding'))

    expt_acknows = Table('experiment_acknowledgment', metadata,
                         PointerCol('experiment'),
                         PointerCol('acknowledgment'))

    metadata.create_all(bind=engine)
    time.sleep(0.1)

    db = SimpleDB(dbname, **conn)
    
    # add some initial data:
    for table, values in (('esaf_status', ESAF_STATUS),
                          ('folder_status', FOLDER_STATUS),
                          ('process_status', PROCESS_STATUS),
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
