import os
import fnmatch
cwd = os.getcwd()
fs = os.listdir(cwd)
for fn in fs:
    if  fnmatch.fnmatch ( fn, 'thread_*[0-9].json' ):
            print fn
    

