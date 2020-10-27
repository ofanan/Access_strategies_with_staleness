import os
import numpy as np

def getTracesPath():
	trace_path_splitted = os.getcwd().split ("\\")
	return (trace_path_splitted[0] + "/" + trace_path_splitted[1] + "/" + trace_path_splitted[2] + "/Documents/traces/") 


def calcOvhDsCost ():
	"""
	Returns the loads of the 19-nodes OVH network, based on the distances and BWs	
	"""
	full_path_to_rsrc   = os.getcwd() + "\\..\\resources\\"
	client_DS_dist_df   = pd.read_csv (full_path_to_rsrc + 'ovh_dist.csv',index_col=0)
	client_DS_dist      = np.array(client_DS_dist_df)
	client_DS_BW_df     = pd.read_csv (full_path_to_rsrc + 'ovh_bw.csv',index_col=0)
	client_DS_BW        = np.array(client_DS_BW_df)
	bw_regularization   = np.max(np.tril(client_DS_BW,-1)) 
	alpha = 0.5
	return 1 + alpha * client_DS_dist + (1 - alpha) * (bw_regularization / client_DS_BW) # client_DS_cost(i,j) will hold the access cost for client i accessing DS j


def exponential_window (old_estimate, new_val, alpha):
	return alpha * new_val + (1 - alpha) * old_estimate 

def bw_to_uInterval (DS_size, bpe, num_of_DSs, bw):
	return int (round (DS_size * bpe * (num_of_DSs-1)) / bw)

def uInterval_to_Bw (DS_size, bpe, num_of_DSs, uInerval):
	return (DS_size * bpe * (num_of_DSs-1)) / uInterval

def get_optimal_num_of_hashes (bpe):
    """
	Returns the optimal number of hash functions for a given number of Bits Per Element (actually, cntrs per element) in a Bloom filter
    """
    return int (bpe * np.log (2))

def calc_designed_fpr (cache_size, BF_size, num_of_hashes):
    return pow (1 - pow (1 - 1/BF_size, num_of_hashes * cache_size), num_of_hashes)
# def get_designed_fpr (hash_cnt):
# 	"""
# 	Returns the designed (inherent) fpr of a simple BF with the given number of Bits Per Element 
# 	"""
# 	return pow (0.5, hash_cnt)