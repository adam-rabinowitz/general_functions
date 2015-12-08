import subprocess
import multiprocessing
import gzip

def writeFromPipe(fileName, pipes, shell = True):
    ''' This fun
    1)  fastqFile - Full path to output FASTQ file
    '''
    # Close unused pipes
    pipes[1].close()
    # Create output gzip file using subprocess
    if shell and fileName.endswith('.gz'):
        # Create outut file
        outFile = gzip.open(fileName, 'wb')
        # Create gzip subprocess
        sp = subprocess.Popen('gzip', stdout = outFile,
            stdin = subprocess.PIPE)
        # Write to output subprocess
        while True:
            try:
                line = pipes[0].recv()
            except EOFError:
                break
            sp.stdin.write(line)
        # Terminate subprocess
        sp.communicate()
    # Or create output file using pure python
    else:
        # Open output file
        if fileName.endswith('.gz'):
            outFile = gzip.open(fileName, 'wb')
        else:
            outFile = open(fileName, 'wb')
        # Write to output file
        while True:
            try:
                line = pipes[0].recv()
            except EOFError:
                break
            outFile.write(line)
    # Close files and pipes
    outFile.close()
    pipes[0].close()

class writeProcess(object):
    ''' Creates a class to handle writing to file in a seperate process.
    Object is intialised with two arguments:
    
    1)  fileName - Full path to output file.
    2)  shell - Boolean, indicating whether gzip fie should be written
        using shell 'gzip' command and the python subprocess module.
    
    The object has two functions:
    
    1)  add - Add object to file.
    2)  close - Close pipe, used to communicate with process, and join
        process.
    
    '''
    
    def __init__(self, fileName, shell = True):
        self.pipes = multiprocessing.Pipe('False')
        self.process = multiprocessing.Process(
            target = writeFromPipe,
            args = (fileName, self.pipes, shell)
        )
        self.process.start()
        self.pipes[0].close()
    
    def add(self, input):
        self.pipes[1].send(input)
    
    def close(self):
        self.pipes[1].close()
        self.process.join()

class write(object):
    ''' Creates a class to handle writing to file. Object is intialised
    with two arguments:
    
    1)  fileName - Full path to output file.
    2)  shell - Boolean, indicating whether gzip fie should be written
        using shell 'gzip' command and the python subprocess module.
    
    The object has three functions:
    
    1)  add - Add object to file.
    2)  close - Close pipe, used to communicate with process, and join
        process.
    
    '''
    
    def __init__(self, fileName, shell = True):
        # Create out file
        if fileName.endswith('.gz'):
            self.file = gzip.open(fileName, 'wb')
        else:
            self.file = open(fileName, 'wb')
        # Write gzip files using shell gzip
        if shell and fileName.endswith('.gz'):
            self.sp = subprocess.Popen('gzip', stdout = self.file,
                stdin = subprocess.PIPE)
            self.output = self.sp.stdin
        # Else write gzip files using gzip module
        else:
            self.sp = None
            self.output = self.file
        # Else write uncompressed file

    def add(self, input):
        self.output.write(input)

    def close(self):
        if self.sp:
            self.sp.communicate()
        self.file.close()
