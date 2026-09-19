"""D-1: refresh real bars, run scheduled-batch DAG, then publish latest replay marts."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker','compose','--env-file','.env.airflow','-f','compose.yaml']


def run(cmd):
    print('Running:',subprocess.list2cmdline(cmd),flush=True)
    subprocess.run(cmd,cwd=ROOT,check=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--stocks-only',action='store_true',help='Refresh candidate archive only, no BigQuery publication')
    args=parser.parse_args()
    run([sys.executable,'scripts/collect_market_universe.py','--activate'])
    if args.stocks_only:
        print('Candidate updated; BigQuery is unchanged. Run run_demo.cmd after batch is ready.')
        return
    run(COMPOSE+['up','-d','airflow'])
    run_id='prepare_demo_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    # CLI availability may lag behind container start; trigger only after CLI is ready.
    for attempt in range(20):
        check=subprocess.run(COMPOSE+['exec','-T','airflow','airflow','dags','list','--output','json'],
                             cwd=ROOT,capture_output=True,text=True)
        if check.returncode==0 and 'jisdor_daily' in check.stdout:
            break
        time.sleep(3)
    else:
        raise RuntimeError('Airflow not ready')
    run(COMPOSE+['exec','-T','airflow','airflow','dags','unpause','jisdor_daily'])
    run(COMPOSE+['exec','-T','airflow','airflow','dags','trigger','jisdor_daily','--run-id',run_id])
    deadline=time.monotonic()+1200
    print('Waiting for batch + date coverage audit:',run_id,flush=True)
    previous_state=None
    last_notice=0
    while time.monotonic()<deadline:
        result=subprocess.run(COMPOSE+['exec','-T','airflow','airflow','dags','list-runs','jisdor_daily','--output','json'],
                              cwd=ROOT,capture_output=True,text=True,timeout=60)
        runs=[]
        if result.returncode==0:
            for line in result.stdout.splitlines():
                if line.strip().startswith('[{'):
                    runs=json.loads(line)
        match=next((r for r in runs if r['run_id']==run_id),None)
        state=match['state'] if match else 'waiting_for_scheduler'
        if state!=previous_state or time.monotonic()-last_notice>30:
            print('Airflow batch:',state,flush=True)
            previous_state,last_notice=state,time.monotonic()
        if match and match['state']=='success':
            break
        if match and match['state']=='failed':
            raise RuntimeError('Batch failed; inspect Airflow. Stock publication not continued.')
        time.sleep(5)
    else:
        raise RuntimeError('Batch timeout; inspect Airflow')
    run([sys.executable,'scripts/run_demo.py'])
    print('READY: latest candidate, batch/date audit, BigQuery marts and dashboard updated. Use run_streaming_demo.cmd on demo day.')


if __name__=='__main__':
    main()
