#!/usr/bin/env python
"""mapper.py"""

import sys
import re

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the line into words using regex
    words = re.split(r"[^A-Za-z]", line.strip().lower())
    # increase counters
    for word in words:
         if len(word) > 0 :
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            print (f'{word}\t1') 
