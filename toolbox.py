import os

def check_var(
    variable, check, mn = None, mx = None, gt = None, lt = None
):
    ''' This fenction performs type and value checks on supplied
    variables. It is intended that this function is used to perform
    check on arguments supplied to funcions.
    
    Args:
        variable: variable to check
        check (str): check to perform must be one of:
            'int' - integer.
            'float' - float.
            'num' - either float or integer.
            'file' - string of an existing file path.
            'exc' - string of an existing executable path.
            'dir' - string of an existing directory path.
            'bool' - boolean.
        mn: minimum values for numeric variables.
        mx: maximum values for numeric variables.
        gt: greater than value for numeric variables.
        lt: less than value for numeric variables.
    
    Returns:
        bool: True if variable is of the expected type and value
        or if the variable is None.
    
    Raises:
        TypeError: if any of the arguments of the wrong type.
        ValueError: if variable is of the wrong value.
    
    '''
    # Return True if variable is None
    if variable is None:
        return(True)
    # Check intger variable
    elif check == 'int':
        if not isinstance(variable, int):
            raise TypeError('%s is not an integer' %(variable))
    # Check float variable
    elif check == 'float':
        if not isinstance(variable, float):
            raise TypeError('%s is not a floating point number' %(variable))
    # Check numeric variable
    elif check == 'num':
        if not isinstance(variable, (int, float)):
            raise TypeError('%s is not numeric' %(variable))
    # Check file variable
    elif check == 'file':
        if not isinstance(variable, str):
            raise TypeError('%s is not a string' %(variable))
        if not os.path.isfile(variable):
            raise TypeError('%s is not a file' %(variable))
    # Check executable variable
    elif check == 'exc':
        if not isinstance(variable, str):
            raise TypeError('%s is not a string' %(variable))
        if not os.path.isfile(variable) and os.access(variable, os.X_OK):
            raise TypeError('%s in not executable' %(variable))
    # Check file variable
    elif check == 'dir':
        if not isinstance(variable, str):
            raise TypeError('%s is not a string' %(variable))
        if not os.path.isdir(variable):
            raise TypeError('%s is not a dir' %(variable))
    # Check string variables
    elif check == 'str':
        if not isinstance(variable, str):
            raise TypeError('%s is not an str' %(variable))
    # Check boolean variables
    elif check == 'bool':
        if not isinstance(variable, bool):
            raise TypeError('%s is not boolean' %(variable))
    # Else raise error if check arguemnt not recognised
    else:
        raise IOError('check argument %s not recognised' %(check))
    # Check numerical arguments
    if check in ['int','float','num']:
        # Check minimum argument
        if isinstance(mn, (int, float)):
            if mn > variable:
                raise ValueError('%s < %s' %(variable, mn))
        elif mn is None:
            pass
        else:
            raise TypeError('mn argument must be numeric')
        # Check maximum value
        if isinstance(mx, (int, float)):
            if mx < variable:
                raise ValueError('%s > %s' %(variable, mx))
        elif mx is None:
            pass
        else:
            raise TypeError('mx argument must be numeric')
        # Check greater than argument
        if isinstance(gt, (int, float)):
            if gt >= variable:
                raise ValueError('%s <= %s' %(variable, gt))
        elif gt is None:
            pass
        else:
            raise TypeError('gt argument must be numeric')
        # Check less than argument
        if isinstance(lt, (int, float)):
            if lt <= variable:
                raise ValueError('%s >= %s' %(variable, gt))
        elif lt is None:
            pass
        else:
            raise TypeError('lt argument must be numeric')
    # Return true if no error raised
    return(True)

def fileDict(
        f, sep = '\t'
    ):
    # Create dictionary to store data
    d = {}
    # Loop through input file
    with open(f, 'r') as infile:
        for line in infile:
            # Check for two columns
            try:
                key, value = line.strip().split(sep)
            except ValueError:
                raise IOError('input file must have two columns')
            # Check for non-duplicate entries
            if key in d:
                raise IOError('duplicate keys provided')
            # Add data to dictionary
            d[key] = value
    # Return value
    return(d)

def factors(number):
    ''' Function returns factors of positive integer. Function takes
    a single argument:
    
    1)  n - A positive integer
    '''
    # Check supplied argument
    check_var(number, 'int', gt = 0)
    # Calculate and return factors
    factors = [i for i in xrange(1, number + 1) if number % i == 0]
    return(factors)

def split_string(string, size):
    ''' Function to split string into chunks of a desired size.
    
    Args:
        string (str): String to split
        size (int): size of chunks to return. Must be an intger > 0.
    
    Returns:
        A list of strings of the specified size. The final element of
        the list will be shorter than the specified size if the length
        of the string is not a multiple of size.

    '''
    # Check arguments
    check_var(string, 'str')
    check_var(size, 'int', gt = 0)
    # Split string into chunks of length n, store in list and return
    chunk_list = [string[i : i + size] for i in range(0, len(string), size)]
    return(chunk_list)

def identical_list(l):
    ''' Function to determine if all elements of a list are identical.
    
    Args:
        l (list): Input list
    
    Returns:
        True if all elements of list identical, False otherwise.
    
    '''
    # Check arguments
    if not isinstance(l, list):
        raise TypeError('Argument must be a list')
    if l.count(l[0]) == len(l):
        return(True)
    else:
        return(False)

def find_monomer(string):
    ''' Function finds shortest monomer of a string.
    
    Args:
        string (str): The input string
    
    Returns:
        Function return a tuple of the following elements:
        monomer (str): Sequence of the monomer.
        count (int): Number of occurences of monomer in input string.
    
    '''
    # Find lengths of monomers that can be generated from sequence
    lengths = factors(len(string))
    # Find shortest monomer
    for l in lengths:
        # Generate monomer
        monomers = split_string(string, l)
        # Return identical monomers
        if identical_list(monomers):
            return(monomers[0], len(monomers))
