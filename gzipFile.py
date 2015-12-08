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

def writeFromPipeProcess(fileName, shell = True):
    # Create pipes and process
    pipes = multiprocessing.Pipe('False')
    process = multiprocessing.Process(
        target = writeFromPipe,
        args = (fileName, pipes, shell)
    )
    process.start()
    pipes[0].close()
    # Return process and pipes
    return(process, pipes[1])
