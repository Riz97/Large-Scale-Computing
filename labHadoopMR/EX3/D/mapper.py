#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
# in the form of <word><\t><count>
for line in sys.stdin:
   print line.strip()   