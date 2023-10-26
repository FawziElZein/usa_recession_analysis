import hook
import prehook
import posthook

def lambda_handler(event, context):
    # df_src_list,df_src_titles = prehook.execute_prehook()
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()