import hook
import prehook
import posthook

prehook.execute_prehook()
hook.execute_hook()
posthook.execute_posthook()