#!/usr/bin/env python
"""

SQLAlchemy wrapping of beamtim database

Main Class for full Database:  BeamtimeDB
"""
import os
import sys
import json
import time
import logging
from pathlib import Path
import numpy as np
from socket import gethostname
from datetime import datetime
import yaml
from charset_normalizer import from_bytes

import epics

from .schema import create_beamtimedb
from .simpledb import SimpleDB, isotime

def get_credentials(envvar='BEAMTIMEDB_CREDENTIALS'):
    """look up credentials file from environment variable"""
    conn = {}
    credfile = os.environ.get(envvar, None)
    if credfile is not None and Path(credfile).exists():
        with open(credfile, 'rb') as fh:
            text = str(from_bytes(fh.read()).best())
            conn = yaml.load(text, Loader=yaml.Loader)
    return conn

def json_encode(val):
    "simple wrapper around json.dumps"
    if val is None or isinstance(val, (str, unicode)):
        return val
    return  json.dumps(val)

def isotime2datetime(xisotime):
    "convert isotime string to datetime object"
    sdate, stime = xisotime.replace('T', ' ').split(' ')
    syear, smon, sday = [int(x) for x in sdate.split('-')]
    sfrac = '0'
    if '.' in stime:
        stime, sfrac = stime.split('.')
    shour, smin, ssec  = [int(x) for x in stime.split(':')]
    susec = int(1e6*float('.%s' % sfrac))
    return datetime(syear, smon, sday, shour, smin, ssec, susec)

def make_datetime(t=None, iso=False):
    """unix timestamp to datetime iso format
    if t is None, current time is used"""
    if t is None:
        dt = datetime.now()
    else:
        dt = datetime.utcfromtimestamp(t)
    if iso:
        return datetime.isoformat(dt)
    return dt


class BeamtimeDB(SimpleDB):
    """
    Main Interface to beamtimeDB
    """
    def __init__(self, dbname=None, server='postgresql', create=False, **kws):
        if dbname is None:
            conndict = get_credentials(envvar='BEAMTIMEDB_CREDENTIALS')
            if 'dbname' in conndict:
                dbname = conndict.pop('dbname')
            if 'server' in conndict:
                server = conndict.pop('server')
            kws.update(conndict)

        self.dbname = dbname
        self.server = server
        self.tables = None
        self.engine = None
        self.session = None
        if create:
            create_beamtimedb(dbname, server=self.server, create=True, **kws)
        SimpleDB.__init__(self, dbname=self.dbname, server=self.server, **kws)

    def create_newdb(self, dbname, connect=False, **kws):
        "create a new, empty database"
        create_beamtimedb(dbname,  **kws)
        if connect:
            time.sleep(0.25)
            self.connect(dbname, **kws)

    def getrow(self, table, name):
        """return named row from a table"""
        return self.get_rows(table, where={'name': name},
                             none_if_empty=True, limit_one=True)

    def commit(self):
        pass

    def add_message(self, text):
        """add entry to message table"""
        self.insert('message', text=text)

    def get_messages(self, order_by='modify_time'):
        """get messages"""
        return self.getrows('messages', order_by=order_by)

    def get_user(self, id=None, badge=None, last_name=None,
                 first_name=None, email=None, orcid=None,
                 affiliation=None):
        """get list of users matching keyword arguments,
        or None"""
        where = {}
        if id is not None:
            where['id'] = id
        if badge is not None:
            where['badge'] = badge
        if last_name is not None:
            where['last_name'] = last_name
        if first_name is not None:
            where['first_name'] = first_name
        if last_name is not None:
            where['last_name'] = last_name
        if email is not None:
            where['email'] = email
        if orcid is not None:
            where['orcid'] = orcid
        if affiliation is not None:
            where['affiliation'] = affiliation
        return self.get_rows('person', where=where, none_if_empty=True)
    
    def add_user(self, first_name, last_name, email, badge,
                 orcid=None, affiliation=None, level=None):
        """add user"""
        kws ={'first_name': first_name, 'last_name': last_name,
              'email': email, 'badge': badge}
        if orcid is not None:
            kws['orcid'] = orcid
        #if affiliation is not None:
        #    inst = self.add_affiliation(affiliation, warn=False)
        #    kws['affiliation'] = inst.id
        cur = self.get_user(**kws)
        if cur is None:
            self.add_row('person', **kws)
            cur = self.get_user(**kws)
        return cur

    def get_institution(self, name, city=None, country=None):
        where = {'name': name}
        if city is not None:
            where['city'] = city
        if country is not None:
            where['country'] = country

        return self.get_row('institution', where=where,
                            none_if_empty=True)

    def add_institution(self, name, city=None, country=None, warn=False):
        cur = self.get_instittuion(name, city=city, country=country)
        if warn and cur is not None:
            print(f"Warning: institution '{name}' exists")
        if cur is None:
            kws = {'name': name}                          
            if city is not None:
                kws['city'] = city
            if country is not None:
                kws['country'] = country

            self.add_row('institution', **kws)
            cur = self.get_instituion(name, city=city, country=country)
        return cur

   
    def _getid(self, table, value, key='name'):
        """ generic  get-id-by-name"""
        kws = {key: value}
        row = self.get_row(table, where=kws)
        if row is None:
            self.add_row(table, **kws)
            row = self.get_row(table, where=kws)
        if row is None:
            raise ValueError(f"could not get or add {table} {key}={value}")
        return row.id

   
    def get_proposal(self, prop_id):
        return self.get_row('proposal',where={'id': prop_id}, none_if_empty=True)

    def add_proposal(self, prop_id, title=None, spokesperson=None):
        print("Add Proposal " , prop_id, title, spokesperson)
        prop = self.get_proposal(prop_id)
        if prop is not None:
            raise ValueError(f"proposal {prop_id} exists")
        print("add proposal....")

    def get_experiment(self, esaf_id):
        return self.get_rows('experiment',where={'id': esaf_id}, none_if_empty=True)
        
    def add_experiment(self, esaf_id, run='2025-1',
                       esaf_status='Pending', esaf_type='GUP',
                       beamline=None, proposal=None,
                       spokesperson=None, beamline_contact=None,
                       users=None, title=None, description=None,
                       start_date=None, end_date=None,
                       user_folder=None, data_doi=None,
                       pvlog_template_file=None, esaf_pdf_file=None,
                       proposal_pdf_file=None):
        
        esaf = self.get_experiment(esaf_id)
        if esaf is not None:
            raise ValueError(f"experiment {esaf_id} exists")
        print("add experiment....")
        sperson = self.get_user(id=spokesperson)

        kws ={'id': esaf_id,
              'run_id': self._getid('run', run),
              'esaf_type_id':  self._getid('esaf_type', esaf_type),
              'esaf_status_id':  self._getid('esaf_status', esaf_status),
              'esaf_type_id':  self._getid('esaf_type', esaf_type),
              'spokesperson_id': spokesperson,
              'beamline_id':  self._getid('beamline', beamline),
              'title': title,
              'description': description,
              'start_date': start_date,
              'end_date': end_date,
              }
        
        self.add_row('experiment', **kws)
        exp_id = self.get_experiment(esaf_id)[0].id
        for uid in users:
            self.add_row('experiment_person', experiment_id=exp_id,
                         person_id=uid)
