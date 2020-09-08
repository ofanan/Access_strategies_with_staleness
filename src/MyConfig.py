import os
import numpy as np

def getTracesPath():
	trace_path_splitted = os.getcwd().split ("\\")
	return (trace_path_splitted[0] + "/" + trace_path_splitted[1] + "/" + trace_path_splitted[2] + "/Documents/traces/") 

def exponential_window (old_estimate, new_val, alpha):
	return alpha * new_val + (1 - alpha) * old_estimate 

def get_optimal_hash_count (bpe):
    """
	Rturns the optimal number of hash functions for a given number of Bits Per Element (actually, cntrs per element) in a Bloom filter
    """
    return int (bpe * np.log (2))
     
