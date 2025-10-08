import os
import time
import logging
from warnings import warn
from datetime import datetime, timedelta
from dateutil.parser import parse as dateparse
from pytz import timezone

from epics import get_pv, caput

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

def filldb_from_apsbss(sector='13', run=None):
    beamlines = BEAMLINES[sector]

    bt_db = BeamtimeDB()
    
    dm_url = bt_db.get_info('DM_APS_DB_WEB_SERVICE_URL')
    os.environ['DM_APS_DB_WEB_SERVICE_URL'] = dm_url
    try:
        bss_server = BSS_Server()
    except:
        raise ValueError(f'cannot connect to APSBSS Server with {dm_url=}')

    if run is None:
        run = bss_server.current_run
    
    current_esafs = bss_server.esafs(sector, run=run)

    for esaf in current_esafs:
        # print('esaf ', esaf.esaf_id, esaf.title)
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
            # print(f'proosal {prefix=}, {beamline=}, {propid=}') 
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
                


def update_pvs(sector='13'):
    beamlines = BEAMLINES[sector]
    bt_db = BeamtimeDB()
    
    dm_url = bt_db.get_info('DM_APS_DB_WEB_SERVICE_URL')
    os.environ['DM_APS_DB_WEB_SERVICE_URL'] = dm_url
    try:
        bss_server = BSS_Server()
    except:
        raise ValueError(f'cannot connect to APSBSS Server with {dm_url=}')
    
    tzone = timezone('America/Chicago')
    cycle = bss_server.current_run

    for prefix, name in beamlines.items():
        get_pv(f"{prefix}proposal:beamline")
        get_pv(f"{prefix}esaf:cycle")
        prop_pv = get_pv(f"{prefix}proposal:id")
        esaf_pv = get_pv(f"{prefix}esaf:id")

        caput(f"{prefix}proposal:beamline", name)
        caput(f"{prefix}esaf:cycle", cycle)

    current_time = datetime.now().astimezone(tzone)
    curr_props = {}
    prop_badges = {}
    for prefix, name in beamlines.items():
        props = bss_server.current_proposals(name)
        # print(f"{prefix} {name} {len(props):d} proposals for this cycle")
        current_prop = None
        for propid, prop in props.items():
            start_time = prop.startDate.astimezone(tzone)
            end_time = prop.endDate.astimezone(tzone)
            if start_time < current_time and current_time < end_time:
                current_prop = propid
                # print(f" Current : {propid=}   {prefix=}, {name=}")
        if current_prop is None:
            current_prop = propid                
        prop = props[current_prop]
        start_date = prop.startDate.isoformat(sep=' ', timespec='seconds')
        end_date = prop.endDate.isoformat(sep=' ', timespec='seconds')        
        curr_props[prefix] = [prop.lastNames, prop.startDate, prop.endDate]
        caput(f"{prefix}proposal:id", str(current_prop))
        caput(f"{prefix}proposal:startDate", start_date)
        caput(f"{prefix}proposal:endDate", end_date)
        caput(f"{prefix}proposal:title", prop.title)
        caput(f"{prefix}proposal:userBadges", ', '.join(prop.badges))
        caput(f"{prefix}proposal:users", ', '.join(prop.lastNames))
        
    # print("Look for ESAFS " , sector)
    # print("Current Proposals: ")
    # for _x, _k in curr_props.items():
    #     print("   ", _x , _k[0])
    # print("####")
    
    for esaf in bss_server.current_esafs(sector):
        start_time = esaf.startDate.astimezone(tzone)
        end_time = esaf.endDate.astimezone(tzone)
        if (start_time < current_time and current_time < end_time and
            end_time-start_time < timedelta(days=50)):
            esaf_badges = [u.badge for u in esaf._users]
            esaf_lnames = [u.lastName for u in esaf._users]
            # print("Current ESAF ", esaf.esaf_id, esaf.title, esaf.sector, esaf.startDate, esaf_lnames)
            lname_score = {_x: 0 for _x in curr_props}
            
            for pr_prefix, pr_data in curr_props.items():
                # print(' test ', pr_prefix, pr_data[0], esaf_lnames)
                for elname in esaf_lnames:
                    if elname in pr_data[0]:
                        lname_score[pr_prefix] += 1
            # print(f'{lname_score=}')
            best_score, best_pref = 0, None
            for pref, val in lname_score.items():
                if val > best_score:
                    best_score, prefix = val, pref
           
            # print("-->> prefix ", prefix, esaf.esaf_id)
            caput(f"{prefix}esaf:id", "%d" % esaf.esaf_id)
            caput(f"{prefix}esaf:title",  esaf.title)            
            caput(f"{prefix}esaf:userBadges",  ', '.join(esaf_badges) )
            caput(f"{prefix}esaf:users",  ', '.join(esaf_lnames))
            caput(f"{prefix}esaf:users_total",  len(esaf._users))
            caput(f"{prefix}esaf:description",  esaf.description)
            caput(f"{prefix}esaf:startDate", esaf.startDate.isoformat(sep=' ', timespec='seconds'))
            caput(f"{prefix}esaf:endDate", esaf.endDate.isoformat(sep=' ', timespec='seconds'))            
    # print(dir(esaf))
        
