import os

def checkArg(
    arg, check, mn = None, mx = None, gt = None, lt = None
):
    # Check argument is not Null
    if arg is None:
        return(True)
    # Check intger arguments
    elif check == 'int':
        # Check arg type
        if not isinstance(arg, int):
            raise TypeError('%s is not an integer' %(arg))
    # Check float arguments
    elif check == 'float':
        # Check arg type
        if not isinstance(arg, float):
            raise TypeError('%s is not a floating point number' %(arg))
    # Check numeric arguments
    elif check == 'num':
        # Check arg type
        if not isinstance(arg, (int, float)):
            raise TypeError('%s is not numeric' %(arg))
    # Check file arguments
    elif check == 'file':
        # Check arg type
        if not isinstance(arg, str):
            raise TypeError('%s is not a string' %(arg))
        # Check presence of file
        if not os.path.isfile(arg):
            raise TypeError('%s is not a file' %(arg))
    # Check file arguments
    elif check == 'dir':
        # Check arg type
        if not isinstance(arg, str):
            raise TypeError('%s is not a string' %(arg))
        # Check presence of file
        if not os.path.isdir(arg):
            raise TypeError('%s is not a dir' %(arg))
    # Check string arguments
    elif check == 'str':
        # Check arg type
        if not isinstance(arg, str):
            raise TypeError('%s is not an str' %(arg))
    # Check boolean arguments
    elif check == 'bool':
        # Check arg type
        if not isinstance(arg, bool):
            raise TypeError('%s is not boolean' %(arg))
    # Else raise error if check arguemnt not recognised
    else:
        raise IOError('check argument must be '\
            'int|float|num|file|dir|str|bool')
    # Check numerical arguments
    if check in ['int','float','num']:
        # Check minimum argument
        if isinstance(mn, (int, float)):
            if mn > arg:
                raise ValueError('%s < %s' %(arg, mn))
        elif mn is None:
            pass
        else:
            raise IOError('mn argument must be numeric')
        # Check maximum value
        if isinstance(mx, (int, float)):
            if mx < arg:
                raise ValueError('%s > %s' %(arg, mx))
        elif mx is None:
            pass
        else:
            raise IOError('mx argument must be numeric')
        # Check greater than argument
        if isinstance(gt, (int, float)):
            if gt >= arg:
                raise ValueError('%s <= %s' %(arg, gt))
        elif gt is None:
            pass
        else:
            raise IOError('gt argument must be numeric')
        # Check less than argument
        if isinstance(lt, (int, float)):
            if lt <= arg:
                raise ValueError('%s >= %s' %(arg, gt))
        elif lt is None:
            pass
        else:
            raise IOError('lt argument must be numeric')
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
