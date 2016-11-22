import collections
import os
import re
import subprocess
import time

class submitJobs(object):
   
    def __init__(self):
        self.commandDict = collections.OrderedDict()
    
    def add(
            self, command, processors = 1, memory = 4, stdout = '/dev/null',
            stderr = '/dev/null', modules = [], depend = []
        ):
        ''' command to add commands to job dictionary.
        
        Args:
            command (str)- Command to submit.
            processors (int)- Number of processors.
            memory (int)- Memory per processor in gigabytes.
            stdout (str)- Full path to stdout.
            stderr (str)- Full path to stderr.
            depend (list)- A list of job ids returned from object.

        '''
        # check modules
        if not isinstance(modules, list):
            raise IOError('modules must be a list')
        for m in modules:
            if not isinstance(m, str):
                raise IOError('modules must be a list of strings')
        # check dependencies
        if not isinstance(depend, list):
            raise IOError('dependencies must be a list')
        for d in depend:
            if d not in self.commandDict:
                raise IOError('dependencies must be present in object')
        # check processor arguments
        if not isinstance(processors, int):
            raise IOError('processors argument must be an integer')
        if processors < 0 or processors > 16:
            raise IOError('processors argument must be >= 1 and <= 16')
        # modify input command
        if isinstance(command, list):
            command = ' '.join(command)
        if '"' in command:
            raise ValueError('Command cannot contain a double quotation mark')
        # check stdout and stderr arguments
        stdoutDir = os.path.dirname(stdout)
        if not os.path.isdir(stdoutDir):
            raise IOError('Stdout directory does not exist')
        stderrDir = os.path.dirname(stderr)
        if not os.path.isdir(stderrDir):
            raise IOError('Stderr directory does not exist')
        # add command to dictionary and return command number
        commandNo = len(self.commandDict)
        self.commandDict[commandNo] = (command, processors, memory, stdout,
            stderr, modules, depend)
        return(commandNo)
    
    def submit(self, verbose = False):
        # Check argument
        if not isinstance(verbose, bool):
            raise ValueError('Verbose argument must be boolean')
        # Create slurm ID list
        slurmJobList = []
        slurmRegx = re.compile('^\\s*Submitted batch job (\\d+)\\s*$')
        # extract commands and parameters
        for commandNo in self.commandDict:
            # Unpack command and parameters
            command, processors, memory, stdout, stderr, modules, depend = (
                self.commandDict[commandNo])
            # Set required modules
            moduleCommand = 'module purge'
            if modules:
                moduleOut = ' '.join(modules)
                moduleCommand += ' && module load {}'.format(' '.join(modules))
            else:
                moduleOut = None
            completeCommand = '{} && {}'.format(moduleCommand, command)
            # create slurm command and add node informaion
            slurmCommand = ['sbatch', '--wrap', '"'+ completeCommand + '"',
                '-c', str(processors), '-o', stdout, '-e', stderr,
                '--mem-per-cpu', str(memory) + 'G']
            # Add dependecies to slurm command
            dependList = []
            for d in depend:
                dependList.append(slurmJobList[d][0])
            if dependList:
                dependString = 'afterok:' + ':'.join(dependList)
                slurmCommand.extend(['-d', dependString])
                dependOut = ' '.join(dependList)
            else:
                dependOut = None
            # Try and submit complete command ten times
            slurmCommand = ' '.join(slurmCommand)
            slurmID = None
            for count in range(10):
                slurmOut = subprocess.check_output(slurmCommand, shell=True)
                slurmMatch = slurmRegx.match(slurmOut)
                if slurmMatch is None:
                    time.sleep(5)
                else:
                    slurmID = slurmMatch.group(1)
                    break
            if slurmID is None:
                raise IOError('Could not submit slurm job')
            # store succesfully commoted commands
            slurmJobList.append((slurmID, command, processors, memory, stdout,
                stderr, moduleOut, dependOut))
            # print commands if requested
            if verbose:
                print(
                    'job id:       {}\n'\
                    'command:      {}\n'\
                    'processors:   {}\n'\
                    'memory:       {}\n'\
                    'stdout:       {}\n'\
                    'stderr:       {}\n'\
                    'modules:      {}\n'\
                    'dependencies: {}\n'.format(
                        slurmID, command, processors, memory, stdout, stderr,
                        str(moduleOut), str(dependOut)
                     )
                )
        # save command to output file
        return(slurmJobList)

def parsePathModule(pmFile):
    # Create output dictionary and open file
    pmDict = {}
    with open(pmFile) as inFile:
        # Extract data from file
        for line in inFile:
            lineData = line.strip().split('\t')
            program = lineData[0]
            # Store paths and modules for each program
            pmDict[(program, 'path')] = lineData[1]
            if len(lineData) == 2:
                pmDict[(program, 'modules')] = []
            else:
                pmDict[(program, 'modules')] = lineData[2:]
    # Return data
    return(pmDict)
