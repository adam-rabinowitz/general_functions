import subprocess
import re
import os

def samtools_sam2bam(inSam, outBam, path = 'samtools', threads = 1, 
    delete = True):
    ''' Function to convert SAM file to BAM using samtools. Function built for
    samtools version 1.2. Function takes 4 arguments:
    
    1)  inSam - Name of input SAM file.
    2)  outBam - Name of output BAM file.
    3)  samtools = Path to Samtools.
    4)  delete - Boolean indicating whether to delete input SAM file.
    
    '''
    # Generate command
    convertCommand = '%s view -bh -@ %s -o %s %s' %(
        path,
        str(threads),
        outBam,
        inSam
    )
    if delete:
        convertCommand += ' && rm %s' %(
            inSam
        )
    # Return command
    return(convertCommand)

def samtools_bam2sam(inBam, outSam, path = 'samtools', delete = True):
    ''' Function to convert BAM files to SAM files using samtools. Function
    built for samtools version 1.2. Function takes 4 arguments:
    
    1)  inSam - Name of input SAM file.
    2)  outBam - Name of output BAM file.
    3)  samtools - Path to Samtools.
    4)  delete - Boolean indicating whether to delete input SAM file.
    
    '''
    # Generate command
    convertCommand = '%s view -h -o %s %s' %(
        samtools,
        outSam,
        inBam
    )
    if delete:
        convertCommand += ' && rm %s' %(
            inSam
        )
    # Return command
    return(convertCommand)

def samtools_index(inBam, path = 'samtools'):
    ''' Function to index BAM files. Function built vor version 1.2 of
    samtools. Function takes 2 arguments:
    
    1)  inBam - Input BAM file
    2)  path - Path to samtools
    
    '''
    # Create and return command
    indexCommand = '%s index %s' %(
        path,
        inBam
    )
    return(indexCommand)

# Define function
def samtools_sort(inFile, outFile, name = False, threads = 1,
    memory = '2G', delete = True, path = 'samtools'):
    ''' Function to sort SAM/BAM files using samtools. Function built for
    samtoools version 1.2. If the output file is a BAM and is not sorted by
    name then the file will also be indexed. Function takes 8 arguments:

    1)  inFile - Name of input file. Must end with '.bam' or '.sam' to
        indicate file type.
    2)  outFile - Name of output file. Must end with '.bam' or '.sam' to
        indicate file type.
    3)  name - Boolean indicating whether to sort by name.
    4)  threads - Number of threads to use in sort.
    5)  memory - Memory to use in each thread.
    6)  delete- Boolean indicating whether to delete input and intermediate
        files.
    7)  path - Path to samtools executable.
    
    '''
    # Check input file
    if inFile.endswith('.bam'):
        outputCommand = ''
    elif inFile.endswith('.sam'):
        outputCommand = samtools_sam2bam(
            inSam = inFile,
            outBam = inFile[:-4] + '.bam',
            path = path,
            delete = delete,
            threads = threads
        )
        inFile = inFile[:-4] + '.bam'
    else:
        raise TypeError('File %s does not end with .bam or .sam' %(
            inFile
        ))
    # Check output file
    if outFile.endswith('.bam'):
        outFormat = 'bam'
    elif outFile.endswith('.sam'):
        outFormat = 'sam'
    else:
        raise TypeError('File %s does not end with .bam or .sam' %(
            outFile
        ))
    # Check and process name argument
    if name:
        nameSort = '-n'
    else:
        nameSort = ''
    # Generate sort command
    sortCommand = [path, 'sort', nameSort, '-m', memory, '-@', str(threads),
        '-o', outFile, '-T', outFile[:-4], '-O', outFormat , inFile]
    sortCommand = filter(None,sortCommand)
    sortCommand = ' '.join(sortCommand)
    # Add sort command to output command
    if outputCommand:
        outputCommand += ' && %s' %(
            sortCommand
        )
    else:
        outputCommand = sortCommand
    # Delete input if required
    if delete:
        outputCommand += ' && rm %s' %(
            inFile
        )
    # Index output if possible
    if not name and outFormat == 'bam':
        outputCommand += ' && %s' %(
            samtools_index(
                inBam = outFile,
                path = path
            )
        )
    # Return command
    return(outputCommand)

def picard_index(inBam, picardPath, javaPath = 'java'):
    ''' Function to index BAM files using the Picard toolkit. Function
    takes three arguments:

    1)  inBam - Name of input BAM
    2)  picardPath - Path to Picard.jar file.
    3)  javaPath - Path to java executable.

    '''
    # Build and return command
    indexCommand = '%s -jar %s BuildBamIndex INPUT=%s OUTPUT=%s' %(
        javaPath,
        picardPath,
        inBam,
        inBam + '.bai'
    )
    return(indexCommand)

def picard_mark_duplicates(inBam, outBam, logFile, picardPath = '',
    javaPath = 'java', removeDuplicates = False,
    delete = True):
    ''' Function to mark duplicates using the picard toolkit.The BAM
    output file is also indexed. Function built for picard-tools version
    1.140 and takes 7 arguments:

    1)  inBam - Input BAM file.
    2)  outBam - Output BAM file.
    3)  logFile - Name of file in which to same output metrics.
    4)  picardPath - Path to picard jar file.
    5)  javaPath - Path to java executable.
    6)  removeDuplicates - Boolean value indicating whether duplicates should be
        removed from the output BAM file.
    7)  delete - Boolean whether to delete input BAM file.
    
    '''
    # Process removeDuplicates option
    if removeDuplicates:
        removeDuplicates = 'REMOVE_DUPLICATES=true'
    else:
        removeDuplicates = 'REMOVE_DUPLICATES=false'
    # Create command
    duplicateCommand = [javaPath, '-jar', picardPath, 'MarkDuplicates',
        'I=' + inBam, 'O=' + outBam, 'M=' + logFile, 'ASSUME_SORTED=true',
        removeDuplicates]
    # merge command
    duplicateCommand = ' '.join(duplicateCommand)
    # delete input if requested
    if delete:
        duplicateCommand += ' && rm %s' %(
            inBam
        )
    # Index duplicates output
    duplicateCommand += ' && %s' %(
        picard_index(
            inBam = outBam,
            picardPath = picardPath,
            javaPath = javaPath
        )
    )
    return(duplicateCommand)

def RNASeqC(inBam, fasta, gtf, rRNA, outDir, outPrefix, seqcPath,
    javaPath = 'java', check = True):
    ''' Function to perform RNASeqC analysis of RNA-Seq aligned BAM.
    Duplicates should be marked, but not deleted from the BAM file.
    Function takes 9 arguments.

    1)  inBam - Input BAM file with duplicates marked.
    2)  fasta - FASTA file containing genome sequence.
    3)  gtf - GTF file locating transcripts within genome.
    4)  rRNA - Interval list file locating rRNA genes.
    5)  outDir - Path to output directory.
    6)  outPrefix - Prefix to use for output files.
    7)  seqcPath - Path to RNASeQC.jar
    8)  javaPath - Path to java executable
    9)  check - Boolean indicating whther to check for required FASTA
        file indices and dictionaries.

    '''
    # Check FASTA file
    if check:
        fai = fasta + '.fai'
        dict = re.sub('\.fa$','.dict',fasta)
        for f in [fai, dict]:
            if not os.path.isfile(f):
                raise IOError('Genome file %s not found' %(f))
    # Create sample data string
    sampleData = "'" + outPrefix + "|" + inBam + '|' + outPrefix + "'"
    # Create command
    seqcCommand = [javaPath, '-jar', seqcPath, '-r', fasta, '-rRNA', rRNA,
        '-t', gtf, '-o', outDir, '-s', sampleData]
    # Join and return command
    seqcCommand = ' '.join(seqcCommand)
    return(seqcCommand)

################################################################################
## gatk_realign_target_creator
################################################################################
''' Function to find regions for gatk local relaignment. Function takes 7
arguments:
1)  inBam - Input BAM file
2)  inVcf - Imput VCF file listing indels. May be gzipped
3)  reference - Fasta file contaning referenc genome sequence
4)  outList - Name of output list file. Should end '.list' or '.interval_list'
5)  path - Path to GATK jar file
6)  threads - Number of threads to use in the analysis
7)  execute - Boolean indicating wether to execute generated command.'''
# Load required modules
import subprocess
# Define function
def gatk_realign_target_creator(
	inBam,
	inVcf,
	reference,
	outList,
	java_path = '/farm/babs/redhat6/software/jdk1.7.0_40/bin/java',
	gatk_path = '/farm/babs/redhat6/software/GenomeAnalysisTK-3.2-2/GenomeAnalysisTK.jar',
	threads = 1,
	execute = False
):
	# Build command
	targetCommand = [java_path, '-jar', gatk_path, '-T', 'RealignerTargetCreator',
		'-I', inBam, '-R', reference, '-known', inVcf, '-o', outList, '-nt',
		str(threads)]
	# Execute or return command
	' '.join(targetCommand)
	return(targetCommand)

##################################################################################
## gatk_realign
##################################################################################
# Load required modules
import subprocess
# define function
def gatk_realign(
	inBam,
	inVcf,
	reference,
	target_intervals,
	outBam,
	java_path = '/farm/babs/redhat6/software/jdk1.7.0_40/bin/java',
	gatk_path = '/farm/babs/redhat6/software/GenomeAnalysisTK-3.2-2/GenomeAnalysisTK.jar',
	execute = False
):
	# Build command
	realignCommand = [java_path, '-jar', gatk_path, '-T', 'IndelRealigner', '-I',
		inBam, '-R', reference, '-known', inVcf, '-targetIntervals',
		target_intervals, '-o', outBam]
	# Join and return command
	realignCommand = ' '.join(realignCommand)
	return(realignCommand)
