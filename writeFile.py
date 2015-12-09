import subprocess
import multiprocessing
import gzip
import os

def writeFromPipe(fileName, pipes, shell = True):
    ''' This fun
    1)  fastqFile - Full path to output FASTQ file
    '''
    pid = os.getpid()
    print pid
    # Close unused pipes
    pipes[1].close()
    # Open output file
    if fileName.endswith('.gz'):
        outFile = gzip.open(fileName, 'wb')
    else:
        outFile = open(fileName, 'w')
    print '%s output file opened' %(pid)
    # Create output gzip file using subprocess
    if shell and fileName.endswith('.gz'):
        # Create gzip subprocess
        sp = subprocess.Popen('gzip', stdout = outFile,
            stdin = subprocess.PIPE, close_fds=True)
        print '%s opened subprocess' %(pid)
        count = 0
        # Write to output subprocess
        while True:
            try:
                line = pipes[0].recv()
            except EOFError:
                print '%s EOFError shell reached' %(pid)
                break
            count += 1
            sp.stdin.write(line)
            print '%s Written line %s' %(pid, count)
        # Terminate subprocess
        sp.communicate()
    # Or create output file using pure python
    else:
        # Write to output file
        while True:
            try:
                line = pipes[0].recv()
            except EOFError:
                print '%s EOFError reached' %(pid)
                break
            outFile.write(line)
    # Close files and pipes
    outFile.close()
    print '%s Output file closed' %(pid)
    pipes[0].close()
    print '%s Process pipes closed' %(pid)

class writeFileProcess(object):
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
        print 'Trying to close pipe'
        self.pipes[1].close()
        print 'Trying to close process'
        self.process.join()
        print 'Closed'

class writeFile(object):
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

    def __del__(self):
        self.close()
