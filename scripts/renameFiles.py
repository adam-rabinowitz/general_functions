'''renameFiles.py

Usage:
    
    renameFiles.py <oldname> <newname> <dir>
        [--nocheck] [--recursive]

'''
# Import required modules
import re
import os
import subprocess
from general_python import docopt
# Extract arguments
args = docopt.docopt(__doc__, version='1.0')

# Create function to rename files
def rename(oldFilePath, oldName, newName, noCheck):
    # Split file
    directory, oldFile = os.path.split(oldFilePath)
    # Create new file name and path
    newFile, count = re.subn(oldName, newName, oldFile)
    if not count:
        return()
    newFilePath = os.path.join(directory, newFile)
    # Rename files automatically
    if noCheck:
        subprocess.check_call(['mv', oldFilePath, newFilePath])
    # Rename files with checking
    else:
        # Print file names
        print "\nOld Name:\n%s\nNew Name:\n%s\nEnter 'y' to rename:" %(
            oldFilePath, newFilePath)
        response = raw_input().strip()
        # Check response
        if response == 'y':
            subprocess.check_call(['mv', oldFilePath, newFilePath])
            print 'File Renamed'
        else:
            print 'File NOT Renamed'

# Rename files if not recursive
if not args['--recursive']:
    for oldFile in os.listdir(args['<dir>']):
        # Skip none files
        oldFilePath = os.path.join(args['<dir>'], oldFile)
        if not os.path.isfile(oldFilePath):
            continue
        # Rename files
        rename(oldFilePath, args['<oldname>'], args['<newname>'], args['--nocheck'])
# Rename files if recursive
else:
    # Loop through all subdirectories and files
    for path, directories, files in os.walk(args['<dir>'], topdown = False):
        # Loop through files and directory names
        for oldFile in files + directories:
            # Rename files
            oldFilePath = os.path.join(path, oldFile)
            rename(oldFilePath, args['<oldname>'], args['<newname>'],
                args['--nocheck'])
