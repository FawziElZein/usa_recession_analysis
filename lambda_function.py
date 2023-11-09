import hook
import prehook
import posthook

def lambda_handler(event=None, context=None):
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()

lambda_handler()