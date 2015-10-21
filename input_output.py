# Class to handle output of function to list
class OutputList(object):
	# Function to initialise list output
	def __init__(self):
		self.output = list()
	# function to add data to list
	def add(self, data):
		self.output.append(data)
	# Function to return list
	def close(self):
		return(self.output)

# Class to handle of function to file
class OutputFile(object):
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

# Function to return output object
def handleOutput(output):
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
