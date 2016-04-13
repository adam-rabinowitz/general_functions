"""softlink.py

Usage:
    
    softlink.py <targdir> <chmod> <linkfiles>...
    
"""
# Extract modules
import os
import subprocess
from general_python import docopt
# Extract arguments
args = docopt.docopt(__doc__,version = 'v1')
# Check output directory
if not os.path.isdir(args['<targdir>']):
    raise IOError('%s is not a directory' %(args['<targdir>']))
# Check chmod argument
try:
    int(args['<chmod>'])
except ValueError:
    raise IOError('%s not covertible to an integer' %(args['<chmod>']))
# Check input files
for lf in args['<linkfiles>']:
    if not os.path.isfile(lf):
        raise IOError('%s is not a file' %(lf))
# Create soft link
for lf in args['<linkfiles>']:
    # Modify permission on original file
    subprocess.check_call(['chmod', args['<chmod>'], lf])
    # Create softlink
    subprocess.check_call(['ln', '-s', lf, args['<targdir>']])
    # Modify permission on link file
    sl = os.path.join(args['<targdir>'], os.path.basename(lf))
    subprocess.check_call(['chmod', args['<chmod>'], sl])
