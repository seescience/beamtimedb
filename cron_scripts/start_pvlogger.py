
import time
import os
import sys
import psutil
from subprocess import Popen
from pathlib import Path
from tabulate import tabulate
from datetime import datetime, timedelta
from pyshortcuts import isotime
from beamtimedb import BeamtimeDB
from epicsapps.pvlogger.pvlogger import (TIMESTAMP_FILE,
                                         check_pvlog_timestamp)


def start_logging_process(folder):
    """start pvlogging in a folder, return Popen result"""
    epapps = Path(sys.executable).parent / 'epicsapps'
    if folder.startswith('/cars6'):
        folder = f'/home/detector{folder}'
    pvlog_path = Path(folder, 'pvlog') 
    config = pvlog_path / 'pvlog.yaml'
    stdout = open(pvlog_path / '_stdout.txt', 'a')
    stderr = open(pvlog_path / '_stderr.txt', 'a')
    args = [epapps.as_posix(), 'pvlogger',  config.as_posix()]
    return Popen(args, cwd=pvlog_path.as_posix(), stdout=stdout, stderr=stderr)

def chmod_cars4(folder):
    """fix permissions on cars4 folder so that any user can write"""
    try:
        if 'cars4' in folder:
            target = Path(folder)
            target.chmod(0o775)
            for p in target.rglob('*'):
                p.chmod(0o775)
    except:
        pass

## select oid, enumlabel,enumtypid::regtype from pg_enum ;
##    oid    |  enumlabel  |     enumtypid
## ----------+-------------+-------------------
##   4578100 | new         | experiment_status
##   4578102 | processed   | experiment_status
##   4578104 | modified    | experiment_status
##  47011104 | not started | pvlog_status
##  47011106 | pending     | pvlog_status
##  47011108 | running     | pvlog_status
##  47011110 | canceled    | pvlog_status
##  47011112 | finished    | pvlog_status
    
def set_experiment_pvlog_process(expt, pid, status='pending'):
    pvlog_row = bt_db.get_rows('pvlog_process',  id=expt.pvlog_process_id,
                             none_if_empty=True, limit_one=True)
    now = datetime.now()
    if pvlog_row is None:
        bt_db.insert('pvlog_process',  pid=pid, heartbeat=now, status=status)
        time.sleep(0.01)
        pvlog_row = bt_db.get_rows('pvlog_process', pid=pid, none_if_empty=True, limit_one=True)
        bt_db.update('experiment',  where={'id': expt.id}, pvlog_process_id=pvlog_row.id)
    else:
        bt_db.update('pvlog_process',  where={'id': expt.pvlog_process_id},
                     pid=pid, heartbeat=now, status=status)
        

def get_pvlog_folder_status(folder):
    # STATUS = 'not started', 'pending', 'running', 'finished', 'canceled'
    status = 'not started'
    pvlog_dir = Path(folder, 'pvlog')
    tsfile = pvlog_dir/ TIMESTAMP_FILE
    yfile  = pvlog_dir/'pvlog.yaml'
    nfiles = len(os.listdir(pvlog_dir))
    if tsfile.exists() and nfiles > 5:
        running = check_pvlog_timestamp(pvlog_dir, timestamp_only=True)
        if running:
            status = 'running'
        else:
            status = 'finished'
    elif yfile.exists():
        status = 'pending'
    return status


bt_db = BeamtimeDB()
curr_run = bt_db.get_info('current_run_id')

now = datetime.now()

current_pids = {}
experiments = bt_db.get_rows('experiment', run_id=int(curr_run), )
pvlog_procs = bt_db.get_rows('pvlog_process')
for expt in experiments:
    if (expt.folder is None or
        expt.start_date > (now + timedelta(days=8)) or
        expt.end_date < (now - timedelta(days=2)) ):
        continue
    if expt.folder.startswith('/cars4'):
        chmod_cars4(expt.folder)

    print(f"# Experiment {expt.id} {expt.folder} {expt.start_date}, {expt.end_date}")        
    if expt.pvlog_process_id in (0, '', None, 'None'):
        print(f"  start pvlog")
        proc = start_logging_process(expt.folder)
        pid = proc.pid
        pvlog_status = 'pending'
        set_experiment_pvlog_process(expt, pid, status='pending')        
        
    else:
        print(f"    check pvlog {expt.folder}")
        pvlog_row = bt_db.get_rows('pvlog_process', id=expt.pvlog_process_id, none_if_empty=True, limit_one=True)
        if pvlog_row is not None:
            pid = pvlog_row.pid
            pvlog_status = pvlog_row.status
            pvlog_folder_status = get_pvlog_folder_status(expt.folder)
            # print(f"        {pvlog_row.pid=}, {pvlog_row.status=} {pvlog_folder_status=}")
            
            if expt.start_date < (now + timedelta(days=1)) and expt.end_date > (now - timedelta(minutes=1)): # should be running
                running = False
                if psutil.pid_exists(pvlog_row.pid):
                    cmdline = ' '.join(psutil.Process(pvlog_row.pid).cmdline())
                    if 'epicsapp' in cmdline and 'pvlogger' in cmdline and expt.folder in cmdline:
                        running = True
                if running:
                    print("    pvlogging appears to be running fine")
                else:
                    print(f"    needs a restart? {pvlog_folder_status=}, {pvlog_row.status=}")
                    time.sleep(2)
                    proc = start_logging_process(expt.folder)
                    pid = proc.pid
                    pvlog_status = 'pending'
                    set_experiment_pvlog_process(expt, pid, status='pending')
                    print(f"    pvlogging restarted {pid=}")
            else:
                    print(f"    experiment does not start for a day or more")                

        else:
            pvlog_status = 'error'
        bt_db.update('pvlog_process',  where={'pid': pid}, status=pvlog_status, heartbeat=datetime.now())
    
        current_pids[pid] = (expt.id, pvlog_status)

out = []
for pid, dat in current_pids.items():
    expt_id, pvlog_status = dat
    expt = bt_db.get_rows('experiment', where={'id': expt_id},
                          none_if_empty=True, limit_one=True)
    if expt is None:
        proc = 'error reading database'
    elif psutil.pid_exists(pid):
        proc = psutil.Process(pid)
    else:
        proc = 'no pvlog proces'
    out.append({'ESAF': expt_id, 'PID': pid, 'Status': pvlog_status, 'Process': proc})

   
print(f'#{isotime()}: Current Processes')
print(tabulate(out, headers='keys', tablefmt='psql'))

