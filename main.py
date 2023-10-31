import hook
import prehook
import posthook
import schedule
import time

def etl_job():
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()

etl_job()
# schedule.every(1).minutes.do(etl_job)

# while True:
#     schedule.run_pending()
#     time.sleep(15)
