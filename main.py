import hook
import prehook
import posthook
import schedule

def etl_job():
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()

schedule.every().day.at("10:00").do(etl_job)

while True:
    schedule.run_pending()