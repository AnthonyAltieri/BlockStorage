import BlockService
import os

DIR = os.path.dirname(os.path.realpath(__file__))


def runTests(tests):
    def createDivider(len):
        def func():
            div = ''
            for i in range(0, len):
                div += '-'
            print div
        return func

    def header(numberTests):
        line = '<><><><><><><><><><><><>'
        box = '[ STARTING ' + numberTests + ' TESTS ]'
        header = line + box + line
        print header
        return len(header)

    headerLength = header(len(tests))
    divider = createDivider(headerLength)

    def run(number, test):
        string = '(TEST ' + number + '/' + str(len(tests)) + ') '
        string += test.name
        string += ' ---> '
        result = test()
        if result:
            string += 'SUCCESS'
        else:
            string += 'FAIL'
        return result

    numCorrect = 0
    for i, test in enumerate(tests):
        if run(i, test):
            numCorrect += 1
        divider()

    lastDiv = ''
    for i in range(0, headerLength):
        lastDiv += '='

    print lastDiv
    print str(numCorrect) + '/' + str(len(tests)) + ' TESTS CORRECT'


# TESTS






