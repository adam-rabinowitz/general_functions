import unittest
import iohandle
import os
import tempfile
import gzip
import multiprocessing
import filecmp


class InputOutputTestCase(unittest.TestCase):
    
    def setUp(self):
        ''' Create required data for unittest '''
        # Create directories and file names
        self.dirName = tempfile.mkdtemp()
        self.testOut = self.dirName + '/test_out.txt'
        self.testOutGZ = self.dirName + '/test_out.txt.gz'
        self.testIn = self.dirName + '/test_in.txt'
        self.testInGZ = self.dirName + '/test_in.txt.gz'
        # Create data
        self.data1 = 'i love cheese'
        self.data2 = ['i', 'fear', 'horses']
        self.data3 = (3, 4, 6)
        self.datalist = [self.data1,self.data2,self.data3]
        # Create files
        testOut = open(self.testIn, 'w')
        testOut.write('%s\n%s\n%s\n' %(
            self.data1,
            '\t'.join(self.data2),
            '\t'.join(map(str,self.data3))
        ))
        testOut.close()
        testOutGZ = gzip.open(self.testInGZ, 'w')
        testOutGZ.write('%s\n%s\n%s\n' %(
            self.data1,
            '\t'.join(self.data2),
            '\t'.join(map(str,self.data3))
        ))
        testOutGZ.close
    
    def tearDown(self):
        ''' Remove temporary files and directories '''
        for fileName in [self.testOut, self.testOutGZ, self.testIn,
            self.testInGZ]:
            if os.path.isfile(fileName):
                os.remove(fileName)
        os.removedirs(self.dirName)


class TestInputOutputHandling(InputOutputTestCase):

    
    def test_list_output(self):
        ''' Test list output '''
        listOut = iohandle.handleout([])
        listOut.add(self.data1)
        listOut.add(self.data2)
        listOut.add(self.data3)
        self.assertEqual(
            listOut.close(),
            self.datalist
        )
    
    def test_file_output(self):
        ''' Test file output '''
        fileOut = iohandle.handleout(self.testOut)
        fileOut.add(self.data1)
        fileOut.add(self.data2)
        fileOut.add(self.data3)
        fileOut.close()
        self.assertTrue(
            filecmp.cmp(
                self.testOut,
                self.testIn
            )
        )
    
    def test_gzfile_output(self):
        ''' Test gzip file output '''
        fileOut = iohandle.handleout(self.testOutGZ)
        fileOut.add(self.data1)
        fileOut.add(self.data2)
        fileOut.add(self.data3)
        fileOut.close()
        # Extrcat data from file
        fileIn = gzip.open(self.testOutGZ, 'r')
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
    
    def test_pipe_output(self):
        ''' Test pipe output '''
        pipeRecv, pipeSend = multiprocessing.Pipe(False)
        pipeOut = iohandle.handleout(pipeSend)
        pipeOut.add(self.data1)
        pipeOut.add(self.data2)
        pipeOut.add(self.data3)
        self.assertEqual(
            [pipeRecv.recv(),pipeRecv.recv(),pipeRecv.recv()],
            self.datalist
        )
    
    def test_type_failure(self):
        ''' Test output type failure '''
        with self.assertRaises(TypeError):
            testOut = iohandle.handleout(3)
    
    def test_file_failure(self):
        ''' Test file data failure '''
        fileOut = iohandle.handleout(self.testOut)
        with self.assertRaises(TypeError):
            fileOut.add(['a','b'],['c','d'])
        fileOut.close()
    
    def test_list_input(self):
        ''' Test list input '''
        listIn = iohandle.handlein(self.datalist)
        self.assertEqual(
            [listIn.next(),listIn.next(),listIn.next()],
            [self.data1,self.data2,self.data3]
        )
        with self.assertRaises(EOFError):
            listIn.next()
    
    def test_file_input(self):
        ''' Test file input '''
        fileIn = iohandle.handlein(self.testIn)
        self.assertEqual(
            [fileIn.next(),fileIn.next(),fileIn.next()],
            [self.data1, self.data2, map(str, self.data3)]
        )
        with self.assertRaises(EOFError):
            fileIn.next()
    
    def test_gzfile_input(self):
        ''' Test gzip file input '''
        fileIn = iohandle.handlein(self.testInGZ)
        self.assertEqual(
            [fileIn.next(),fileIn.next(),fileIn.next()],
            [self.data1, self.data2, map(str, self.data3)]
        )
        with self.assertRaises(EOFError):
            fileIn.next()
    
    def test_pipe_input(self):
        ''' Test pipe input '''
        pipeRecv, pipeSend = multiprocessing.Pipe(False)
        pipeSend.send(self.data1)
        pipeSend.send(self.data2)
        pipeSend.send(self.data3)
        pipeSend.send(None)
        pipeIn = iohandle.handlein(pipeRecv)
        self.assertEqual(
            [pipeIn.next(),pipeIn.next(),pipeIn.next()],
            self.datalist
        )
        with self.assertRaises(EOFError):
            pipeIn.next()

suite = unittest.TestLoader().loadTestsFromTestCase(TestInputOutputHandling)
unittest.TextTestRunner(verbosity=3).run(suite)
