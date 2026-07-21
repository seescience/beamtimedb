import os
import time
import logging
from warnings import warn
from datetime import datetime, timedelta
from dateutil.parser import parse as dateparse
from dateutil.tz import gettz
from pytz import timezone

from epics import get_pv, caput

from .beamtimedb import BeamtimeDB

try:
    # from apsbss.server_interface import Server as BSS_Server
    from dm.aps_db_web_service.api.esafApsDbApi import EsafApsDbApi
    from dm.aps_db_web_service.api.bssApsDbApi import BssApsDbApi
except ImportError:
    warn('cannot import dm.aps_db_web_service to read APS BSS data')

BEAMLINES = {'13': {'13IDE:bss:': '13-ID-E',
                    '13IDCD:bss:': '13-ID-C,D',
                    '13BMD:bss:': '13-BM-D',
                    '13BMC:bss:': '13-BM-C'}
             }

APSBSS_BEAMLINES = {1: ('13-BM-C', '13BMC:bss:'),
                    2: ('13-BM-D', '13BMD:bss:'),
                    3: ('13-ID-C,D', '13IDCD:bss:'),
                    4: ('13-ID-E', '13IDE:bss:')}

TZ = gettz('America/Chicago')

def get_current_esafs(bssapi, esafapi):
    current_run = bssapi.getCurrentRun()
    run = current_run['name']
    year = run.split('-')[0]
    out = []
    run_start = dateparse(current_run['startTime']).astimezone(TZ)
    run_end   = dateparse(current_run['endTime']).astimezone(TZ)
    print(f"BSS CURRENT {current_run=}")
    
    year_esafs = esafapi.listStationEsafs('GSECARS', year=year)
    for esaf in year_esafs:
        start_date =  dateparse(esaf['experimentStartDate']).astimezone(TZ)
        end_date =  dateparse(esaf['experimentEndDate']).astimezone(TZ)

        if start_date > run_start and start_date < run_end:
            out.append(esaf)
    return out
    

def filldb_from_apsbss(sector='13', run=None):
    beamlines = BEAMLINES[sector]

    bt_db = BeamtimeDB()
    
    # dm_url = bt_db.get_info('DM_APS_DB_WEB_SERVICE_URL')
    # os.environ['DM_APS_DB_WEB_SERVICE_URL'] = dm_url
    try:
        bssapi = BssApsDbApi()
    except:
        raise ValueError(f'cannot connect to APSBSS Server')
    try:
        esafapi = EsafApsDbApi()
    except:
        raise ValueError(f'cannot connect to APSBSS Server')

    if run is None:
        run = bssapi.getCurrentRun()
    
    current_esafs = get_current_esafs(bssapi, esafapi) 
    print("Current ESAFS ", len(current_esafs))
    for esaf in current_esafs:
        # print("ESAF ", esaf['esafId'], esaf['esafTitle'])
        esaf_id = int(esaf['esafId'])
        
        user_ids = []
        spokesperson = None
        for user in esaf['experimentUsers']:
            d_user = bt_db.get_user(badge=user['badge'])
            if d_user is None:
                d_user = bt_db.add_user(badge=int(user['badge']),
                                        last_name=user['lastName'],
                                        first_name=user['firstName'],
                                        email=user['email'])
            user_ids.append(d_user.id)
            if user['piFlag'] in ('Yes', True):
                spokesperson = d_user.id

        if  bt_db.get_experiment(esaf_id) is None:
            print("Add ESAF: " , esaf_id, esaf['esafTitle'])

            start_date = dateparse(esaf['experimentStartDate']).astimezone(TZ)
            end_date = dateparse(esaf['experimentEndDate']).astimezone(TZ)
            aps_doi = esaf.get('doi', '')
            
            bt_db.add_experiment(esaf_id, run=run['name'],
                                 esaf_status=esaf['esafStatus'],
                                 start_date=start_date,
                                 end_date=end_date,
                                 title=esaf['esafTitle'],
                                 description=esaf['description'],
                                 spokesperson=spokesperson,
                                 aps_doi=aps_doi,
                                 users=user_ids)
        aps_doi = esaf.get('doi', '')
        if len(aps_doi) > 8:
            expt = bt_db.get_experiment(esaf_id)
            expt_aps_doi = str(expt.aps_doi)
            if expt_aps_doi in (None, 'None', '') or len(expt_aps_doi)< 8:
                print("Set DOI for experiment ", esaf_id, esaf['esafTitle'])
                bt_db.update('experiment', where={'id': esaf_id},
                             aps_doi=aps_doi)
            
    print("- end esaf -------")
    # proposals
    for prop in bssapi.listStationProposals('GSECARS', runName=run['name']):
        spokesperson = None
        for user in prop['experimenters']:
            badge = int(user['badge'])
            affil = user['institution']
            inst = bt_db.get_institution(name=affil)
            if inst is None:
                inst = bt_db.add_institution(affil)
            b_user = bt_db.get_user(badge=badge)
            if b_user is None:
                if 'email' not in user:
                    user['email'] = 'unknown'
                print("Add User ", user['email'])
                b_user = bt_db.add_user(user['firstName'], user['lastName'],
                                         user['email'], badge)

            bt_db.update('person', where={'badge': badge}, affiliation_id=inst.id)
            if user['piFlag'] in (True, 'Y', 'y'):
                spokesperson = b_user.id

        propid = prop['id']
        title = prop['title']
        if title.endswith('\n'):
            title = title[:-1]
        kws = {'title': title}
        if spokesperson is not None:
            kws['spokesperson_id'] = spokesperson
        b_prop = bt_db.get_proposal(propid)
        if b_prop is None:
            print("Add Proposal ", propid, title, kws)
            b_prop = bt_db.add_proposal(propid, **kws)
            

def update_pvs():
    beamlines = BEAMLINES['13']
    bt_db = BeamtimeDB()
    
    bssapi = BssApsDbApi()
    current_run = bssapi.getCurrentRun()
    run = current_run['name']
    print(f"Updating PVs for {run=}")

    run_id = int(bt_db.get_info('current_run_id'))
    expt_list = bt_db.get_rows('experiment', where={'run_id': run_id})

    # complicated to find current esafs
    current_time = datetime.now().astimezone(TZ)
    maybe_current = {k: [] for k in APSBSS_BEAMLINES}

    for expt in expt_list:
        start_time = expt.start_date.astimezone(TZ)
        end_time = expt.end_date.astimezone(TZ)
        bl_id = int(expt.beamline_id)
        if bl_id > 0 and bl_id < 7:
            if ((start_time > (current_time - timedelta(days=4))) and
                (start_time < (current_time + timedelta(days=2))) and
                (end_time   < (current_time + timedelta(days=10))) and
                (end_time   > (current_time - timedelta(hours=4)))):
                maybe_current[bl_id].append(expt)

    # print("# Proposals that may be Current:")                
    for blid, elist in maybe_current.items():
        for expt in elist:
            user = bt_db.get_user(expt.spokesperson_id)
            #print(expt.id, expt.beamline_id, expt.proposal_id,
            #      expt.start_date, expt.end_date, expt.spokesperson_id,
            #      user.last_name, expt.title)
                
    current_esaf = {k: None for k in APSBSS_BEAMLINES}    
    for blid, elist in maybe_current.items():
        if len(elist) == 1:
            current_esaf[blid] = elist[0]
        elif len(elist) > 1:
            for expt in elist:
                start_time = expt.start_date.astimezone(TZ)
                end_time = expt.end_date.astimezone(TZ)
                if ((start_time < (current_time+timedelta(hours=3))) and (end_time   > (current_time))):
                    current_esaf[blid] = expt
                    # print("Set current proposal ", blid, expt.proposal_id)                    
            if current_esaf[blid] is None:
                ex0 = elist[0]
                st0 = ex0.start_date.astimezone(TZ)
                for expt in elist[1:]:
                    start_time = expt.start_date.astimezone(TZ)
                    if start_time < st0:
                        ex0 = expt
                        st0 = ex0.start_date.astimezone(TZ)
                current_esaf[blid] = ex0


    # Noe set PVs
    for blid, expt in current_esaf.items():
        blname, prefix = APSBSS_BEAMLINES[blid]
        print(f"# {blname}")
        caput(f"{prefix}proposal:beamline", blname)        
        caput(f"{prefix}esaf:cycle", run)
        if expt is not None:
            user_badges = []
            user_names = []
            suser = bt_db.get_user(expt.spokesperson_id)
            user_badges.append(f'{suser.badge}')
            user_names.append(suser.last_name)
            for userdat in bt_db.get_rows('experiment_person',
                                         where={'experiment_id': expt.id}):
                user = bt_db.get_user(userdat.person_id)
                if user.badge not in user_badges:
                    user_badges.append(f'{user.badge}')
                if user.last_name not in user_names:
                    user_names.append(user.last_name)
            info = {'expt_id': str(expt.id),
                    'title': expt.title,
                    'start_time':  expt.start_date.isoformat(sep=' ',
                                                             timespec='seconds'),
                    'users': ', '.join(user_names)}
            caput(f"{prefix}esaf:id", str(expt.id))
            caput(f"{prefix}esaf:startDate", expt.start_date.isoformat(sep=' ', timespec='seconds'))
            caput(f"{prefix}esaf:endDate", expt.end_date.isoformat(sep=' ', timespec='seconds'))
            caput(f"{prefix}esaf:title",  expt.title)            
            caput(f"{prefix}esaf:description",  expt.description)
            caput(f"{prefix}esaf:userBadges",  ', '.join(user_badges) )
            caput(f"{prefix}esaf:users",  ', '.join(user_names))
            caput(f"{prefix}esaf:users_total",  len(user_names))

            prop = bt_db.get_proposal(expt.proposal_id)
            caput(f"{prefix}proposal:title", prop.title)
            caput(f"{prefix}proposal:id", str(expt.id))
            for key, val in info.items():
                print(f"   {key}: {val}")
            
def old_pvput():      
    prop_badges = {}
    for prefix, name in beamlines.items():
        props = bssapi.listStationProposals('GSECARS', runName=run)
        print("props " , props)
        # print(f"{prefix} {name} {len(props):d} proposals for this cycle")
        current_prop = None
        for prop in props:
            start_time = prop.startDate.astimezone(TZ)
            end_time = prop.endDate.astimezone(TZ)
            if start_time < current_time and current_time < end_time:
                current_prop = prop['     id']
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
        start_time = esaf.startDate.astimezone(TZ)
        end_time = esaf.endDate.astimezone(TZ)
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
        
