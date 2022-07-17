#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_count = 0
acc = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    # <count> <1>
    count, _ = line.split('\t')
    
    # convert count (currently a string) to int
    try:
        count = int(count)
       
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    
    # if first value
    if current_count == 0:
        current_count = count
    
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_count == count:
        acc += 1
    
    else:
        print '%d\t%d'%(current_count,acc)
        current_count = count
        acc = 1
        
# do not forget to output the last record <-- 
if current_count == count:
    print '%d\t%d'%(current_count,acc)