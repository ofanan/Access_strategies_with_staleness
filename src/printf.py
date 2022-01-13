"""
An accessory function for format-printing to a file.
"""
from __future__ import print_function

def printf(output_file, str, *args):
    print(str % args, end='', file = output_file, flush = True)
