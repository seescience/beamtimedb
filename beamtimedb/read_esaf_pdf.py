"""
extract text from ESAF PDF

Hopefully not needed 
"""
import PyPDF2

def read_esaf_header(filename):
    """return dictionary of data from the top of the 
    first page of an ESAF PDF
    """
    pdf_reader = PyPDF2.PdfReader(open(filename, mode='rb'))
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

