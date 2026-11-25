# This code was previously in schema.py.
# It is referenced by both schema.py and api.py.
from datetime import datetime

from sqlalchemy import Column, MetaData, Text, DateTime, Table, Integer, String, Boolean, ForeignKey

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

metadata = MetaData()

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

folder_status = Table('folder_status', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', String(64)))                       

insts = Table('institution', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(2048)),
                Column('city', String(512)),
                Column('country', String(512)) )

funding = Table('funding', metadata,
                Column('id', Integer, primary_key=True),
                Column('source',      String(512)),
                Column('details',     String(512)),
                Column('grant_number', String(512)),
                Column('percentage',  Integer) )

prop_fund = Table('proposal_funding', metadata,
                PointerCol('proposal'),
                PointerCol('funding'))


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
                    Column('pvlog_template', Text),
                    PointerCol('beamline'),
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
                    PointerCol('spokesperson', other='person'),
                    StrCol('proposal_call'),
                    StrCol('proposal_type'),
                    StrCol('co_pi'),
                    StrCol('primary_area'),
                    StrCol('additional_areas'),
                    StrCol('review_panel'),
                    StrCol('run_cycle'),
                    StrCol('status'),
                    Column('submitted_date', DateTime),
                    StrCol('beamlines'),
                    StrCol('pdf_file'))

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
                    Column('needs_doi', Boolean, default=True),
                    Column('needs_folder', Boolean, default=True),
                    Column('needs_pvlog', Boolean, default=True),
                    StrCol('pvlog_file'),
                    IntCol('etr_number'),
                    )

# join tables for many-to-one relations
expt_user = Table('experiment_person', metadata,
                    PointerCol('experiment'),
                    PointerCol('person'),
                    PointerCol('user_type'))

expt_staff = Table('experiment_staff', metadata,
                    PointerCol('experiment'),
                    PointerCol('person'))

expt_tech = Table('experiment_technique', metadata,
                    PointerCol('experiment'),
                    PointerCol('technique'))

expt_fund = Table('experiment_funding', metadata,
                    PointerCol('experiment'),
                    PointerCol('funding'))

expt_acknows = Table('experiment_acknowledgment', metadata,
                        PointerCol('experiment'),
                        PointerCol('acknowledgment'))