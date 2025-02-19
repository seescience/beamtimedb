from apsbss.server_interface import Server as BSS_Server
from beamtimedb import BeamtimeDB

bt_db = BeamtimeDB()

bss = BSS_Server()
sector = '13'
beamlines = {'13IDE:bss:': '13-ID-E',
             '13IDCD:bss:': '13-ID-C,D',
             '13BMD:bss:': '13-BM-D',
             '13BMC:bss:': '13-BM-C'
             }


for esaf in bss.current_esafs(sector):
    # print(dir(esaf))
    print(esaf.esaf_id, esaf.run, esaf.status,
          esaf.startDate,  esaf.endDate,  esaf.title)
    user_ids = []
    spokesperson = None
    for user in esaf._users:
        d_user = bt_db.get_user(badge=user.badge)
        if d_user is None:
            print("Adding User ", user)
            d_user = bt_db.add_user(badge=int(user.badge), last_name=user.lastName,
                                    first_name=user.firstName, email=user.email)
        user_ids.append(d_user.id)
        # print(user, user.institution_id)
        if user.is_pi:
            spokesperson = d_user.id
            
    # print("USER IDS ", user_ids, spokesperson)
    dexp = bt_db.get_experiment(esaf.esaf_id)
    if dexp is None:
        bt_db.add_experiment(esaf.esaf_id, run=esaf.run, esaf_status=esaf.status,
                             start_date=esaf.startDate, end_date=esaf.endDate,
                             title=esaf.title, description=esaf.description,
                             spokesperson=spokesperson, users=user_ids)

        
# proposals
print("################################")
for prefix, beamline in beamlines.items():
    print(beamline)
    for propid, prop in bss.current_proposals(beamline).items():
        print(propid, beamline, prop)
        print(dir(prop))
   
        print(propid, prop.proposal_id, prop.run, prop.startDate, prop.endDate, prop.badges)
        spokesperson = None
        for user in prop.to_dict()['experimenters']:
            badge = int(user['badge'])
            affil = user['institution']
            inst = bt_db.get_institution(name=affil)
            if inst is None:
                inst = bt_db.add_institution(affil)

            b_user = bt_db.get_user(badge=badge)
            if b_user is None:
                print("Adding User ", user)
                b_user = bt_db.add_user(user['firstName'], user['lastName'], user['email'], badge)

            bt_db.update('person', where={'badge': badge}, affiliation_id=inst.id)
            if user['piFlag'] in (True, 'Y', 'y'):
                spokesperson = b_user.id

        title = prop.title
        if title.endswith('\n'): title = title[:-1]
        kws = {'title': title}
        if spokesperson is not None:
            kws['spokesperson_id'] = spokesperson
        b_prop = bt_db.get_proposal(propid)
        if b_prop is None:
            b_prop = bt_db.add_proposal(propid, **kws)
                
