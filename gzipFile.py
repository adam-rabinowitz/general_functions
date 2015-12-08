import subprocess

def writeFromPipe(fileName, pipes, shell = True):
    ''' Write FASTQ or FASTA file 
    1)  fastqFile - Full path to output FASTQ file
    '''
    # Close unused pipes
    pipes[1].close()
    # Create output file and use as stdout of subprocess
    if shell:
        # Create gzip subprocess
        fileOut = gzip.open(fileName, 'wb')
        sp = subprocess.Popen('gzip', stdout = fileOut,
            stdin = subprocess.PIPE)
        # Write to output file
        while True:
            try:
                line = pipes[0].recv()
            except EOFError:
                break
            sp.stdin.write(line)
        # Close subprocess, files and pipes
        sp.communicate()
        fileOut.close()
        pipes[0].close()
    # Or create output file using pure python
    else:
        # Create output file
        outFile = gzip.open(fileName, 'w')
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
        args = (fastqFile, pipes)
    )
    process.start()
    pipes[0].close()
    # Return process and pipes
    return(process, pipes[1])
