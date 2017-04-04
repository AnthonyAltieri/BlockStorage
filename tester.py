import subprocess
import os

DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = DIR + '/baseDir'

def bash(command):
    return subprocess.check_output(['bash', '-c', command])

def runClient(command, file):
    return bash('./runClient.sh ./config_file ' + BASE_DIR + ' ' + command + ' ' + file)



Four_MB = 4194304
BLOCK_SIZE = 4


bash('rm -rf baseDir')

bash('mkdir baseDir')

toUpload = open('./toUpload', 'w+')
filler = 'AnthonyAltieri'
END_BOUND = BLOCK_SIZE
for i in range(END_BOUND):
    toUpload.write(filler)

file1 = open('./baseDir/file1', 'w+')
for i in range(1000):
    file1.write('|file1|')


fileA = open('./fileA', 'w+')
for i in range(0, BLOCK_SIZE):
    fileA.write('A')

file2A = open('./file2A', 'w+')
for i in range(0, BLOCK_SIZE * 2):
    fileA.write('A')


fileAB = open('./fileAB', 'w+')
for i in range(0, BLOCK_SIZE):
    fileAB.write('A')
for i in range(0, BLOCK_SIZE):
    fileAB.write('B')
print "Created fileA and fileAB"
exit()

print "<><><><><><><><>[ STARTING TESTS ]<><><><><><><><><>"
print "Test 1: UPLOAD FILE"
output = runClient('upload', DIR + '/toUpload')
if output != "OK":
    print "[FAILED]"
    print "runClient('upload', '" + DIR + '/toUpload' + ') produced output != OK:'
    print output
print "output: " + output
print "----------------------------------------------------"
print "Test 2: DOWNLOAD FILE"
output = runClient('download', 'toUpload')
if output != "OK":
    print "[FAILED]"
    print "runClient('download', 'toUpload') produced output != OK:"
    print output
print "output " + output
output = bash('diff ./toUpload ./baseDir/toUpload')
if len(output) > 0:
    print "\n[FAILED]"
    print "diff output:"
    print output
else:
    print '\n[SUCCESS]'
print "----------------------------------------------------"



