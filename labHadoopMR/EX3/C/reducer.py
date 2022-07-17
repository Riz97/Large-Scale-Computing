#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys


max = 0
max_words = []

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    # <count> <word>
    values  = line.split('\t')
    
    # convert count (currently a string) to int
    try:
        key = int(values[0])
        if key <= max:
            continue
    except ValueError:
        continue
    
    if key <= max:
        continue
    
    else:
        max = key
        max_words = values
           
        
print '%s'%("\t".join(max_words))