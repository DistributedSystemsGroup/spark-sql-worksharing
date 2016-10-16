# Script for generating random data for the Micro-benchmark

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
    output = ""
    x = 10
    for i in range(0, N_INT):
        if x < 1000000:
            x *= 10
        output += (str(randint(1, x)) + " ")

    x = str(randomDouble(0, 1))
    for i in range(0, N_DOUBLE):
        output += (x + " ")

    word = randomWord(20)
    for i in range(0, N_STR):
        output += (word + " ")

    print(output.strip())

id = 1

def genRecordRef():
    global id
    output = ""
    output += (str(id) + " ")
    x = 10
    for i in range(1, N_INT):
        if x < 1000000:
            x *= 10
        output += (str(randint(1, x)) + " ")

    x = str(randomDouble(0, 1))
    for i in range(0, N_DOUBLE):
        output += (x + " ")

    word = randomWord(20)
    for i in range(0, N_STR):
        output += (word + " ")

    print(output.strip())
    id += 1

nRecords = int(sys.argv[1])
start_time = time.time()
for i in range(0, nRecords):
    genRecord()

eprint("--- %s seconds ---" % (time.time() - start_time))
