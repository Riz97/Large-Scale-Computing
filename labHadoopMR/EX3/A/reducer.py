#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_words = []
current_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    # <count> <word>
    count, word = line.split('\t')
    
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
        current_words.append(word)
    
    else:
        print '%d\t%s'%(current_count,"\t".join(current_words))
        current_count = count
        current_words = [word]
        
# do not forget to output the last record <-- 
if current_count == count:
    print '%d\t%s'%(count,"\t".join(current_words))