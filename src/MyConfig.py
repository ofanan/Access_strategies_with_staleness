import os
import numpy as np
import python_simulator as sim

def calc_service_cost_of_opt (accs_cost, comp_miss_cnt, missp, req_cnt):
	"""
	Opt's behavior is not depended upon parameters such as the indicaror's size, and miss penalty.
	Hence, it suffices to run Opt only once per trace and network, and then calculate its service cost for other 
	parameters' values using this func'
	"""
	return (accs_cost + comp_miss_cnt * missp) / req_cnt

def getTracesPath():
	user_name = os.getcwd().split ("\\")[2]
	return 'C:/Users/' + user_name + '/Google Drive/Comnet/traces/'
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
	settings_str = '{}.C{:.0f}K.bpe{:.0f}.{:.0f}Kreq.{:.0f}DSs.Kloc{:.0f}.M{:.0f}.B{:.0f}.U{:.0f}.' .format (
		trace_file_name, DS_size/1000, bpe, num_of_req/1000, num_of_DSs, k_loc, missp, bw, uInterval)
	if (alg_mode == sim.ALG_OPT):
		return settings_str + 'Opt'
	elif (alg_mode == sim.ALG_PGM_FNO):
		return settings_str + 'FNO'
	elif (alg_mode == sim.ALG_PGM_FNA_MR1_BY_HIST):
		return settings_str + 'FNA'

def calc_designed_fpr (cache_size, BF_size, num_of_hashes):
    return pow (1 - pow (1 - 1/BF_size, num_of_hashes * cache_size), num_of_hashes)
# def get_designed_fpr (hash_cnt):
# 	"""
# 	Returns the designed (inherent) fpr of a simple BF with the given number of Bits Per Element 
# 	"""
# 	return pow (0.5, hash_cnt)