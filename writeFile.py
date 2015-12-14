import subprocess
import multiprocessing
import gzip

class writeFile(object):
    ''' Creates a class to handle writing to file. Object is intialised
    with two arguments:
    
    1)  fileName - Full path to output file.
    2)  shell - Boolean, indicating whether gzip fie should be written
        using shell 'gzip' command and the python subprocess module.
    
    The object has two callable functions:
    
    1)  add - Add supplied string to file.
    2)  close - Close pipe, process and output file.
    
    '''
    
    def __init__(self, fileName, shell = True):
        # Create out file
        if fileName.endswith('.gz'):
            self.file = gzip.open(fileName, 'wb')
        else:
            self.file = open(fileName, 'wb')
        # Create output object
        if shell and fileName.endswith('.gz'):
            self.sp = subprocess.Popen('gzip', stdout = self.file,
                stdin = subprocess.PIPE)
            self.output = self.sp.stdin
        else:
            self.sp = None
            self.output = self.file
    
    def __enter__(self):
        return(self)
    
    def add(self, input):
        self.output.write(input)
    
    def close(self):
        if self.sp:
            self.sp.communicate()
        self.file.close()
    
    def __del__(self):
        self.close()
    
    def __exit__(self, type, value, traceback):
        self.close()


class writeFileProcess(object):
    ''' Creates a class to handle writing to file in a seperate process.
    Object is intialised with two arguments:
    
    1)  fileName - Full path to output file.
    2)  shell - Boolean, indicating whether gzip fie should be written
        using shell 'gzip' command and the python subprocess module.
    
    The object has two primary functions:
    
    1)  add - Add supplied string to file.
    2)  close - Close pipes, process and file
    
    '''
    
    # Store parental pipe ends
    pipeSendList = []
     
    def __init__(self, fileName, shell = True):
        # Store supplied parameters
        self.fileName = fileName
        self.shell = shell
        # Create pipes
        self.pipes = multiprocessing.Pipe('False')
        # Create and start process
        self.process = multiprocessing.Process(target = self.writeFromPipe)
        self.process.start()
        # Process pipes
        self.pipes[0].close()
        writeFileProcess.pipeSendList.append(self.pipes[1])
    
    def writeFromPipe(self):
        ''' Function to write to gzip files within a python
        multiprocessing process.
        '''
        # Close unused pipes
        self.pipes[1].close()
        for p in writeFileProcess.pipeSendList:
            p.close()
        # Open output file
        if self.fileName.endswith('.gz'):
            outFile = gzip.open(self.fileName, 'wb')
        else:
            outFile = open(self.fileName, 'w')
        # Create output gzip file using subprocess
        if self.shell and self.fileName.endswith('.gz'):
            # Create gzip subprocess
            sp = subprocess.Popen('gzip', stdout = outFile,
                stdin = subprocess.PIPE, close_fds=True)
            # Write to output subprocess
            while True:
                try:
                    line = self.pipes[0].recv()
                except EOFError:
                    break
                sp.stdin.write(line)
            # Terminate subprocess
            sp.communicate()
        # Or create output file using pure python
        else:
            # Write to output file
            while True:
                try:
                    line = self.pipes[0].recv()
                except EOFError:
                    break
                outFile.write(line)
        # Close files and pipes
        outFile.close()
        self.pipes[0].close()
    
    def __enter__(self):
        return(self)
    
    def add(self, input):
        self.pipes[1].send(input)
    
    def close(self):
        self.pipes[1].close()
        self.process.join()
    
    def __del__(self):
        self.close()
    
    def __exit__(self, type, value, traceback):
        self.close()
