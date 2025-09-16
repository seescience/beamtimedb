"""
extract some information from text from ESAF PDF
"""

from glob import glob
from pathlib import Path

from pypdf import PdfReader
from .beamtimedb import BeamtimeDB

def read_esaf_header(filename):
    """return dictionary of data from the top of the 
    first page of an ESAF PDF
    """
    print(f" READ PDF HEADER {filename=}")
    pdf_reader = PdfReader(open(filename, mode='rb'))
    page1_text = pdf_reader.pages[0].extract_text()
   
    data =  {'printed_date': None,
             'beamline': None,
             'pen_line': None,
             'pen_key': None,
             'experiment_id': None,
             'proposal_id': None,
             'start_datetime': None,
             'end_datetime': None,
             'spokesperson': None,
             'experiment_type': None}
    
    for line in page1_text.split('\n'):
        if line.startswith('Printed date:'):
            words = line.split(':')
            data['printed_date'] = words[1].strip()
        elif line.startswith('PEN:'):
            data['pen_line'] = line
            if 'Experiment ID:' in line:
                words = [s.strip() for s in line[4:].split('Experiment ID:')]
                data['pen_key'] = words[0]
                bwords = words[0].split('-')
                data['beamline'] = '-'.join(bwords[:2])
                ewords = words[1].split()
                data['experiment_id'] = ewords[0].strip()
                data['experiment_type'] = ewords[1].strip().replace('(','').replace(')','')
        elif line.startswith('ID Start Date:'):
            xline = line.replace('ID Start Date:', '')
            words = xline.split('ID End Date:')
            data['start_datetime'] = words[0].strip()
            data['end_datetime'] = words[1].strip()
        elif line.startswith('BM Start Date:'):
            xline = line.replace('BM Start Date:', '')
            words = xline.split('BM End Date:')
            data['start_datetime'] = words[0].strip()
            data['end_datetime'] = words[1].strip()

        elif line.startswith('Spokesperson:'):
            xline = line.replace('Spokesperson:', '')
            words = xline.split('GUP ID:')
            data['spokesperson'] = words[0].strip()
            data['proposal_id'] = words[1].strip()
    return data


BLNAMES = None

def get_beamline_names():
    """ lookup beamline names"""
    global BLNAMES
    beamdb = BeamtimeDB()
    BLNAMES = {}
    for row in beamdb.get_rows('apsbss_beamline'):
        if row.name is not None:
            name = row.name.lower().replace('-', '').replace(',', '')
            BLNAMES[name] = row.id
    return beamdb

def match_beamline(blname):
    """ match a beamline name, returning database ID for that beamline"""
    global BLNAMES
    if BLNAMES is None:
        get_beamline_names()
        
    name = blname.lower().replace('-', '').replace(',', '')
    bid = BLNAMES.get(name, None)
    if name == '13idd' or name == '13idc':
        bid = BLNAMES.get('13idcd', None)        
    if bid is None: 
        for key, xbid in BLNAMES.items():
            if key.startswith(name):
                bid = xbid
    if bid is None:
        bid = BLNAMES.get('unknown', None)
    return bid

def read_esaf_pdfs(run=None):
    beamdb = get_beamline_names()
    esaf_folder = beamdb.get_info('esaf_pdf_folder')
    if run is None:
        run_id = beamdb.get_info('current_run_id')
        run_name = beamdb.get_rows('run', where={'id': int(run_id)},
                                   limit_one=True, none_if_empty=True)
        run_name = run_name.name
    else:
        run_name = run
        run = beamdb.get_rows('run', where={'name': run},
                                   limit_one=True, none_if_empty=True)
        run_id = run.id
        
    for bname in ('BMC', 'BMD',  'ID CD', 'IDE'):
        folder = Path(esaf_folder, run_name, bname)
        for pdffile in glob(folder.as_posix() + '/*'):
            if not pdffile.endswith('.pdf'):
                continue
                
            data = read_esaf_header(pdffile)
            # print("READ PDF ", pdffile, '\n', data)
            if data['beamline'] is None:
                continue
            bl_id = match_beamline(data['beamline'])
            proprow = beamdb.get_row('proposal', where={'id': int(data['proposal_id'])})
            if proprow is not None:
                beamdb.update('experiment', where={'id': int(data['experiment_id'])},
                              proposal_id=int(data['proposal_id']), beamline_id=bl_id,
                              run_id=run_id)

              
