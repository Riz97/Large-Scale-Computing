#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

n_words = 0
f_key   = 0

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()
    # parse the input we got from mapper.py
    # <key> <words> or <count> <numofwords>
    values = line.split('\t')
    try:
       n_words = int(values[1])
       f_key = int(values[0]) 
    except ValueError:
        words = '\t'.join(values[1:])
        if f_key == int(values[0]):
            print "%d\t%s" % (n_words, words)