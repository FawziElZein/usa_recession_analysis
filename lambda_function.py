import hook
import prehook
import posthook

def lambda_handler(event, context):
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()
