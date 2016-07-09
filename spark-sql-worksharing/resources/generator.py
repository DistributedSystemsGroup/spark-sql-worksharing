from random import randint
from random import uniform
import random, string
import sys
import time

N_INT = 10
N_DOUBLE = 10
N_STR = 10

def randomWord(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def eprint(str):
    sys.stderr.write(str)

def randomDouble(min, max):
    return format(uniform(min, max), '.2f')

def genRecord():
    integerFields = []
    doubleFields = []
    strFields = []
    n1 = randint(1, 100)
    integerFields.append(n1)
    n2 = randint(1, 1000)
    integerFields.append(n2)
    for i in range(2, N_INT):
        integerFields.append(randint(1, 10000))

    d1 = randomDouble(0, 1)
    doubleFields.append(d1)
    d2 = randomDouble(0, 2)
    doubleFields.append(d2)
    for i in range(2, N_DOUBLE):
        doubleFields.append(randomDouble(1, 2))

    for i in range(0, N_STR):
        doubleFields.append(randomWord(20))

    fields = integerFields + doubleFields + strFields
    output = ""
    for item in fields:
        output += (str(item) + " ")
    print(output.strip())


def genRecord2():
    integerFields = []
    doubleFields = []
    strFields = []
    i1 = randint(1, 100)
    integerFields.append(i1)

    f1 = randomDouble(0, 1)
    doubleFields.append(f1)

    str1 = randomWord(20)
    strFields.append(str1)

    fields = integerFields + doubleFields + strFields
    output = ""
    for item in fields:
        output += (str(item) + " ")
    print(output.strip())


nRecords = int(sys.argv[1])
start_time = time.time()
for i in range(0, nRecords):
    genRecord()

eprint("--- %s seconds ---" % (time.time() - start_time))