import os
import numpy as np
import python_simulator as sim
import pandas as pd

# This file contains several accessory functions, used throughout the project.


## Reduces the memory print of the trace by using the smallest type that still supports the values in the trace
## Note: this configuration can support up to 2^8 locations, and traces of length up to 2^32
def reduce_trace_mem_print(trace_df, k_loc):
    new_trace_df = trace_df
    new_trace_df['req_id']    = trace_df['req_id'].astype('uint32')        # id (running cnt number) of the request
    new_trace_df['key']       = trace_df['key'].astype('uint32')              # key. No need for value in the sim'
    new_trace_df['client_id'] = trace_df['client_id'].astype('uint8')   # client to which this request is assigned
    # max_k_loc = min (num_of_DSs, 5)
    #    for i in range (max_k_loc):
    #        new_trace_df['kloc%d' %i] = trace_df['kloc%d' %i].astype('uint8')   # client to which this request is assigned
    for i in range (k_loc):
        new_trace_df['%d'%i] = trace_df['%d'%i].astype('uint8')
    return new_trace_df

def  gen_requests (trace_file_name, max_num_of_req, k_loc=1):
    return reduce_trace_mem_print (pd.read_csv (getTracesPath() + trace_file_name).head(max_num_of_req), k_loc)



# Sizes of the bloom filters (number of cntrs), for chosen DS sizes, k=5 hash funcs, and designed false positive rate.
# The values are taken from https://hur.st/bloomfilter
# online resource calculating the optimal values
def optimal_BF_size_per_DS_size ():
    BF_size_for_DS_size = {}
    BF_size_for_DS_size[0.01] = {20: 197, 40: 394, 60: 591, 80: 788, 100: 985, 200: 1970, 400: 3940, 600: 5910, 800: 7880, 1000: 9849, 1200: 11819, 1400: 13789, 1600: 15759, 2000: 19698, 2500: 24623, 3000: 29547}
    BF_size_for_DS_size[0.02] = {20: 164, 40: 328, 60: 491, 80: 655, 100: 819, 200: 1637, 400: 3273, 600: 4909, 800: 6545, 1000: 8181, 1200: 9817, 1400: 11453, 1600: 13089}
    BF_size_for_DS_size[0.03] = {1000: 7299}
    BF_size_for_DS_size[0.04] = {1000: 6711}
    BF_size_for_DS_size[0.05] = {20: 126, 40: 251, 60: 377, 80: 502, 100: 628, 200: 1255, 400: 2510, 600: 3765, 800: 5020, 1000: 6275, 1200: 7530, 1400: 8784, 1600: 10039}
    return BF_size_for_DS_size 


def calc_service_cost_of_opt (accs_cost, comp_miss_cnt, missp, req_cnt):
	"""
	Opt's behavior is not depended upon parameters such as the indicaror's size, and miss penalty.
	Hence, it suffices to run Opt only once per trace and network, and then calculate its service cost for other 
	parameters' values using this func'
	"""
	return (accs_cost + comp_miss_cnt * missp) / req_cnt

def getTracesPath():
    """
    returns the path in which the traces files are found at this machine.
    This path should be:
    C:/Users/userName/Google Drive/Comnet/traces
    """
    if (os.getcwd().split ("\\")[0] == "C:"):
        user_name = os.getcwd().split ("\\")[2]
        return 'C:/Users/' + user_name + '/Google Drive/Comnet/traces/'
    else:
        return ('/home/itamarq/itamarq/traces/')
# 	trace_path_splitted = os.getcwd().split ("\\")
# 	return (trace_path_splitted[0] + "/" + trace_path_splitted[1] + "/" + trace_path_splitted[2] + "/Documents/traces/") 


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
	"""
	Given a requested bw [bits / system request], returns the per-cache uInterval, namely, the avg num of events this cache has to count before sending an update.
	An "event" here is either a user access to the cache, or an insertion to that cache.
	The simulator calculates the number of events implicitly, by assuming that each user request causes an event to a single cache -- 
	and hence, each cache sees an event once in num_of_DSs user requests on average. 
	"""
	return int (round (DS_size * bpe * num_of_DSs * (num_of_DSs-1)) / bw)
 
def uInterval_to_Bw (DS_size, bpe, num_of_DSs, uInerval):
	return (DS_size * bpe * num_of_DSs * (num_of_DSs-1)) / uInterval

def get_optimal_num_of_hashes (bpe):
    """
	Returns the optimal number of hash functions for a given number of Bits Per Element (actually, cntrs per element) in a Bloom filter
    """
    return int (bpe * np.log (2))


def settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode):
    """
    Returns a formatted string based on the values of the given parameters' (e.g., num of caches, trace_file_name, update intervals etc.). 
    """
    # homo_or_hetro = 'homo' if is_homo else 'hetro'
    settings_str = '{}.C{:.0f}K.bpe{:.0f}.{:.0f}Kreq.{:.0f}DSs.Kloc{:.0f}.M{:.0f}.B{:.0f}.U{:.0f}.' .format (
		trace_file_name, DS_size/1000, bpe, num_of_req/1000, num_of_DSs, k_loc, missp, bw, uInterval)
    if (alg_mode == sim.ALG_OPT):
        return settings_str + 'Opt'     
    elif (alg_mode == sim.ALG_PGM_FNO):
        return settings_str + 'FNO'
    elif (alg_mode == sim.ALG_PGM_FNA_MR1_BY_HIST):
        return settings_str + 'FNA'
    elif (alg_mode == sim.ALG_PGM_FNA_MR1_BY_ANALYSIS):
        return settings_str + 'FNAA'
    elif (alg_mode == sim.ALG_MEAURE_FP_FN):
        return settings_str + 'measure_fn'

def calc_designed_fpr (cache_size, BF_size, num_of_hashes):
    """
    returns the designed (inherent) fpr of a BF, based on the given cache size, BF size, and number of hash functions used by the BF.
    """
    return pow (1 - pow (1 - 1/BF_size, num_of_hashes * cache_size), num_of_hashes)
