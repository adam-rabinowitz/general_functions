import unittest
import input_output as io
import os
import tempfile
import gzip
import multiprocessing

class OutputTestCase(unittest.TestCase):
	def setUp(self):
		self.dirName = tempfile.mkdtemp()
		self.fileName = self.dirName + '/test.txt'
		self.fileNameGZ = self.dirName + '/test.txt.gz'
		self.data1 = 'i love cheese'
		self.data2 = ['i', 'fear', 'horses']
		self.data3 = (3, 4, 6)
	def tearDown(self):
		if os.path.isfile(self.fileName):
			os.remove(self.fileName)
		if os.path.isfile(self.fileNameGZ):
			os.remove(self.fileNameGZ)
		os.removedirs(self.dirName)

class TestOutputHandling(OutputTestCase):

	# Test list output
	def test_list_output(self):
		listOut = io.handleOutput([])
		listOut.add(self.data1)
		listOut.add(self.data2)
		listOut.add(self.data3)
		self.assertEqual(
			listOut.close(),
			[self.data1,self.data2,self.data3]
		)

	# Test file output
	def test_file_output(self):
		# Add data to file and close
		fileOut = io.handleOutput(self.fileName)
		fileOut.add(self.data1)
		fileOut.add(self.data2)
		fileOut.add(self.data3)
		fileOut.close()
		# Extrcat data from file
		fileIn = open(self.fileName, 'r')
		fileContents = fileIn.readlines()
		fileIn.close()
		# Check file is as expected
		self.assertEqual(
			fileContents,
			[
				self.data1 + '\n',
				'\t'.join(self.data2) + '\n',
				'\t'.join(map(str,self.data3)) + '\n'
			]
		)

	# Test gzip file output
	def test_gzfile_output(self):
		# Add data to file and close
		fileOut = io.handleOutput(self.fileNameGZ)
		fileOut.add(self.data1)
		fileOut.add(self.data2)
		fileOut.add(self.data3)
		fileOut.close()
		# Extrcat data from file
		fileIn = gzip.open(self.fileNameGZ, 'r')
		fileContents = fileIn.readlines()
		fileIn.close()
		# Check file is as expected
		self.assertEqual(
			fileContents,
			[
				self.data1 + '\n',
				'\t'.join(self.data2) + '\n',
				'\t'.join(map(str,self.data3)) + '\n'
			]
		)

	# Test pipe output
	def test_pipe_output(self):
		pipeRecv, pipeSend = multiprocessing.Pipe(False)
		pipeOut = io.handleOutput(pipeSend)
		pipeOut.add(self.data1)
		pipeOut.add(self.data2)
		pipeOut.add(self.data3)
		self.assertEqual(
			[pipeRecv.recv(),pipeRecv.recv(),pipeRecv.recv()],
			[self.data1,self.data2,self.data3]
		)

	# Test output type failure
	def test_type_failure(self):
		with self.assertRaises(TypeError):
			testOut = io.handleOutput(3)

	# Test file output failure
	def test_file_failure(self):
		fileOut = io.handleOutput(self.fileName)
		with self.assertRaises(TypeError):
			fileOut.add(['a','b'],['c','d'])
		fileOut.close()

suite = unittest.TestLoader().loadTestsFromTestCase(TestOutputHandling)
unittest.TextTestRunner(verbosity=3).run(suite)
