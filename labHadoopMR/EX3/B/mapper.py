#!/usr/bin/env python
"""mapper.py"""

import sys
import re

# input comes from STDIN (standard input)
# in the form of <word><\t><count>
for line in sys.stdin:
    word, count = line.split('\t')
    word = word.strip()
    if len(word) > 0 :
        try:
            count = int(count)
            
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            print "int cast failed!"
            continue
        
        # swapping key-value to take advantage of the
        # map reduce framework shuffle reordering by key
        print '%d\t%s' % (count, word)
