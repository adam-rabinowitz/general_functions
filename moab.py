# Import required modules
import subprocess
import time
import re
import collections
from general_python import toolbox

def submitJob(
        command, processor = 1, stdout = '/dev/null',
        stderr = '/dev/null', dependency = None
    ):
    ''' This function submits jobs to the farm using the MOAB software
    package and return the MOAB job identifier. The function takes 5
    arguments:
    
    1)  command - The system command to submit to the farm.
    2)  processor - Number of processors to run the job on (Default = 1).
    3)  stdout - File in which to save STDOUT stream (Default =
        /dev/null).
    4)  stderr - File in which to save STERR stream (Default =
        /dev/null).
    5)  dependency - MOAB job IDs of jobs that have to be sucessfully
        completed before processing current command (Default = None).
    
    IF 'stdout' and 'stderr' are identical then the two streams are
    combined and saved in a single file. Function returns the MOAB ID
    of the submitted job.
    '''
    # Modify input command
    if isinstance(command, list):
        command = ' '.join(command)
    # Create msub command
    msubCommand = ['/opt/moab/bin/msub', '-l']
    # Add node information
    nodeCommand = 'nodes=1:babs:ppn=%s' %(processor)
    msubCommand.append(nodeCommand)
    # Add output information
    if stdout == stderr:
        msubCommand.extend(['-j', 'oe', '-o', stdout])
    else:
        msubCommand.extend(['-o', stdout, '-e', stderr])
    # Add dependecy
    if dependency:
        if isinstance(dependency, list):
            dependency = 'x=depend:afterok:%s' %(':'.join(dependency))
        elif isinstance(dependency, str):
            dependency = 'x=depend:afterok:%s' %(dependency)
        msubCommand.extend(['-W', dependency])
    # Create output variable for function
    moabID = None
    # Try submit the job ten times
    for _ in range(10):
        # Create msub process
        msubProcess = subprocess.Popen(
            msubCommand,
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )
        # Submit command
        moab = msubProcess.communicate(input=command)[0]
        # Search for returned Moab ID
        moabMatch = re.match('^\s+(Moab\\.\d+)\s+$',moab)
        # Check that Moab ID has been returned or wait and repeat
        if moabMatch:
            moabID = moabMatch.group(1)
            break
        else:
            time.sleep(10)
    # Stop process and return moabID
    return(moabID)

class moabJobs(object):
    
    def __init__(self):
        self.commandDict = collections.OrderedDict()
    
    def add(
            self, command, processors = 1, stdout = '/dev/null',
            stderr = '/dev/null', dependency = []
        ):
        ''' Command to add commands to job dictionary.'''
        # Check dependencies
        if isinstance(dependency, list):
            for d in dependency:
                if d not in self.commandDict:
                    raise IOError('Dependencies must be present in object')
        else:
            raise IOError('Dependencies must be provided as a list')
        # Check processor arguments
        if not isinstance(processors, int):
            raise IOError('processors argument must be an integer')
        if processors < 0 and processors > 12:
            raise IOError('processors argument must be >= 1 and <= 12')
        # Modify input command
        if isinstance(command, list):
            command = ' '.join(command)
        # Check stdout and stderr arguments
        if not stdout.startswith('/'):
            raise IOError('stdout argument must be an absolute filepath')
        if not stderr.startswith('/'):
            raise IOError('stderr argument must be an absolute filepath')
        # Add command to dictionary and return command number
        commandNo = len(self.commandDict)
        self.commandDict[commandNo] = (
            command,
            processors,
            stdout,
            stderr,
            dependency
        )
        return(commandNo)
    
    def submit(self, verbose = False):
        # Check arguments
        toolbox.checkArg(verbose, 'bool')
        # Create moab list and dictionary
        moabList = []
        # Extract commands and parameters
        for commandNo in self.commandDict:
            # Unpack command and parameters
            command, processors, stdout, stderr, dependency = (
                self.commandDict[commandNo])
            # Create msub command and add node informaion
            msubCommand = ['/opt/moab/bin/msub', '-l']
            msubCommand.append('nodes=1:babs:ppn=%s' %(processors))
            # Add output information
            if stdout == stderr:
                msubCommand.extend(['-j', 'oe', '-o', stdout])
            else:
                msubCommand.extend(['-o', stdout, '-e', stderr])
            # Add dependecy
            dependList = []
            for d in dependency:
                dependList.append(moabList[d][0])
            if dependList:
                depend = 'x=depend:afterok:%s' %(':'.join(dependList))
                msubCommand.extend(['-W', depend])
            # Create output variable for function
            moabID = None
            # Try submit the job ten times
            for _ in range(10):
                # Create msub process
                msubProcess = subprocess.Popen(
                    msubCommand,
                    stdin = subprocess.PIPE,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.PIPE
                )
                # Submit command
                moab = msubProcess.communicate(input=command)[0]
                # Search for returned Moab ID
                moabMatch = re.match('^\s+(Moab\\.\d+)\s+$',moab)
                # Check that Moab ID has been returned or wait and repeat
                if moabMatch:
                    moabID = moabMatch.group(1)
                    break
                else:
                    time.sleep(10)
            # Store succesfully commoted commands
            if moabID:
                moabList.append((moabID, command, processors, stdout, stderr,
                    ' '.join(dependList)))
            else:
                raise IOError('Could not submit moab job')
            # Print commands if requested
            if verbose:
                print 'JOB ID: %s\nCOMMAND: %s\nPROCESSORS: %s\nSTDOUT: %s\n'\
                    'STDERR: %s\nDEPENDENCIES: %s\n' %(moabID, command,
                    processors, stdout, stderr, ' '.join(dependList))
        # Save command to output file
        return(moabList)
