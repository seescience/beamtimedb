import os
from warnings import warn
from .beamtimedb import BeamtimeDB

try:
    from apsbss.server_interface import Server as BSS_Server
except ImportError:
    warn('need to import APSBSS Server to read APS BSS data')


BEAMLINES = {'13': {'13IDE:bss:': '13-ID-E',
                    '13IDCD:bss:': '13-ID-C,D',
                    '13BMD:bss:': '13-BM-D',
                    '13BMC:bss:': '13-BM-C'}
             }


def filldb_from_apsbss(sector='13'):
    beamlines = BEAMLINES[sector]

    bt_db = BeamtimeDB()
    
    dm_url = bt_db.get_info('DM_APS_DB_WEB_SERVICE_URL')
    os.environ['DM_APS_DB_WEB_SERVICE_URL'] = dm_url
    try:
        bss_server = BSS_Server()
    except:
        raise ValueError(f'cannot connect to APSBSS Server with {dm_url=}')

    current_esafs = bss_server.current_esafs(sector)

    esaf_folder_root = bt_db.get_info('esaf_pdf_folder')

    for esaf in current_esafs:
        print('esaf ', esaf.esaf_id, esaf.title)
        user_ids = []
        spokesperson = None
        for user in esaf._users:
            d_user = bt_db.get_user(badge=user.badge)
            if d_user is None:
                d_user = bt_db.add_user(badge=int(user.badge), last_name=user.lastName,
                                        first_name=user.firstName, email=user.email)
            user_ids.append(d_user.id)
            if user.is_pi:
                spokesperson = d_user.id

        if  bt_db.get_experiment(esaf.esaf_id) is None:
            bt_db.add_experiment(esaf.esaf_id, run=esaf.run, esaf_status=esaf.status,
                                 start_date=esaf.startDate, end_date=esaf.endDate,
                                 title=esaf.title, description=esaf.description,
                                 spokesperson=spokesperson, users=user_ids)

        
    # proposals
    for prefix, beamline in beamlines.items():
        for propid, prop in bss_server.current_proposals(beamline).items():
            print(f'proosal {prefix=}, {beamline=}, {propid=}') 
            spokesperson = None
            for user in prop.to_dict()['experimenters']:
                badge = int(user['badge'])
                affil = user['institution']
                inst = bt_db.get_institution(name=affil)
                if inst is None:
                    inst = bt_db.add_institution(affil)

                b_user = bt_db.get_user(badge=badge)
                if b_user is None:
                    if 'email' not in user:
                        user['email'] = 'unknown'
                    b_user = bt_db.add_user(user['firstName'], user['lastName'],
                                            user['email'], badge)

                bt_db.update('person', where={'badge': badge}, affiliation_id=inst.id)
                if user['piFlag'] in (True, 'Y', 'y'):
                    spokesperson = b_user.id

            title = prop.title
            if title.endswith('\n'):
                title = title[:-1]
            kws = {'title': title}
            if spokesperson is not None:
                kws['spokesperson_id'] = spokesperson
            b_prop = bt_db.get_proposal(propid)
            if b_prop is None:
                b_prop = bt_db.add_proposal(propid, **kws)
                
