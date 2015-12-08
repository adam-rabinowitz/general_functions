import subprocess
import multiprocessing
import gzip

def writeFromPipe(fileName, pipes, shell = True):
    ''' Write FASTQ or FASTA file 
    1)  fastqFile - Full path to output FASTQ file
    '''
    # Close unused pipes
    pipes[1].close()
    # Create outut file
    outFile = gzip.open(fileName, 'wb')
    # Create output file and use as stdout of subprocess
    if shell:
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
        # Write to output file
        while True:
            try:
                read = pipes[0].recv()
            except EOFError:
                break
            outFile.write(read)
    # Close files and pipes
    outFile.close()
    pipes[0].close()

class writeProcessObject(object):
    ''' Creates a class to handle writing output to a gzip file in a
    seperate process. Object is intialised with two arguments:

    1)  fileName - Full path to output file.
    2)  shell - Boolean, indicating whether gzip fie should be written
        using the shell 'gzip' command.

    The object has two functions:

    1)  write - Add the supplied string to output file.
    2)  close - Close pipe, used to communicate with process, and join
        process
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
