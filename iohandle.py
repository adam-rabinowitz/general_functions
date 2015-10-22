''' This module is built to handle the output of functions. The use of
the 'handle_output' function within a function allows that function to
return output as a list or to a file or down a multiprocessing pipe.
'''


class OutputList(object):
    ''' Generates an object to handle the output of function as a list.
    Object is initialised with a list. The two defined functions 'add'
    and 'close' add elements to the list and return the list
    respectively.
    '''
    
    # Function to initialise list output
    def __init__(self):
        self.output = list()
    
    # function to add data to list
    def add(self, data):
        self.output.append(data)
    
    # Function to return list
    def close(self):
        return(self.output)


class OutputFile(object):
    ''' Generates an object to handle the output of function to a file.
    Object is initialised by opening a file using the supplied file
    name. The function 'add' writes a supplied object as a line in the
    file. If the object is a list or tuple then the the indvidual
    elements are converted to strings and joined with an intervening
    tab. Otherwise, the object is converted to a string and added to
    the file. The function 'close' closes the file and returns the file
    handle.
    '''
    
    # Function to initialise file output
    def __init__(self, output):
        if output[-3:] == '.gz':
            import gzip
            self.output = gzip.open(output, 'w')
        else:
            self.output = open(output, 'w')
    
    # Function to add data to file
    def add(self, data):
        if isinstance(data, list) or isinstance(data, tuple):
            data = map(str, data)
            self.output.write('\t'.join(data) + '\n')
        else:
            data = str(data)
            self.output.write(data + '\n')
    
    # Function to close and return file handle
    def close(self):
        self.output.close()
        return(self.output)


# Class to handle output of function to multiprocessing pipe
class OutputPipe(object):
    ''' Generates a class to handle output of a function to a pipe.
    Object is initialised with an open pipe. The function 'add' sends
    an object down the pipe. The function 'close' closes the pipe and
    returns it.
    '''
    
    # Function to intialise pipe output
    def __init__(self, output):
        self.output = output
    
    # Function to add data to pipr
    def add(self, data):
        self.output.send(data)
    
    # Function to close and return pipe handle
    def close(self):
        self.output.close()
        return(self.output)


def handle_output(output):
    ''' Function takes a single argument termed 'output'. If 'output'
    is a string then a file output object (OutputFile) is returned. If
    the 'output' is a list then a list output object (OutputList) is
    returned. If 'output' is a pipe then a pipe output object
    (OutputPipe) is returned. All objects have the functions 'add' and
    'close'.
    '''
    if isinstance(output, str):
        outObject = OutputFile(output)
    elif isinstance(output, list):
        outObject = OutputList()
    else:
        from _multiprocessing import Connection
        if isinstance(output, Connection):
            outObject = OutputPipe(output)
        else:
            raise TypeError('Output must be a file-name, list or pipe')
    return(outObject)
