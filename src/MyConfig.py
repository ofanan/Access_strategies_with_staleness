"""
This file contains several accessory functions, used throughout the project. In particular:
- Parse a trace, and associate each request with a concrete client and/or concrete DSs, to which the item is inserted, in case of a miss.
- Generate from the parsed trace a dataframe, to be used as an input to the simulator.
- Calculate the service cost of an optimal alg'.
- Calculate the BW used by the advertisement mechanism. 
- Generate the settings string, which is used to convey all the parameters (# of caches, cache size, indicator size etc.) of a concrete run.
- Calculate the inherent fpr of an optimally-configured Bloom filter, given its number of bits-per-element.
"""
import os
import numpy as np
import python_simulator as sim
import pandas as pd

# Codes for access algorithms
ALG_OPT                         = 1  # Optimal access strategy (perfect indicator)
ALG_PGM_FNO_MR1_BY_HIST         = 7  # PGM alg', detailed in Access Strategies journal paper; False-negative-Oblivious. The exclusion probabilities (mr1) are calculated by the history.
ALG_PGM_FNO_MR1_BY_ANALYSIS     = 8  # PGM alg', detailed in Access Strategies journal paper; False-negative-Oblivious. The exclusion probabilities (mr1) are calculated an analysis of the Bloom filter, as detailed in ICDCS paper. 
ALG_PGM_FNA_MR1_BY_HIST         = 11 # PGM alg', detailed in Access Strategies journal paper; False-negative-Aware. The exclusion probabilities (mr1) are calculated by the history.
ALG_PGM_FNA_MR1_BY_ANALYSIS     = 12 # PGM alg', detailed in Access Strategies journal paper; False-negative-Aware. The exclusion probabilities (mr1) are calculated an analysis of the Bloom filter, as detailed in ICDCS paper.
ALG_PGM_FNA_MR1_BY_HIST_ADAPT   = 13 # PGM alg', detailed in Access Strategies journal paper; staleness-aware, with adaptive alg'
ALG_MEAURE_FP_FN                = 20 # Run a single cache with an always-believe-indicator access strategy, to measure the fpr, fnr, as func' of the update interval.

# levels of verbose
CNT_FN_BY_STALENESS = 5

INF_INT = 999999999

def reduce_trace_mem_print(trace_df, k_loc=1, read_clients_from_trace=False, read_locs_from_trace=False):
    """
    Reduces the memory print of the trace by using the smallest type that still supports the values in the trace
    Note: this configuration can support up to 2^8 locations, and traces of length up to 2^32

    """
    new_trace_df        = trace_df
    new_trace_df['key'] = trace_df['key'].astype('uint32')   
    if (read_clients_from_trace): # need to read the client assigned for each request in the input file?
        new_trace_df['client_id'] = trace_df['client_id'].astype('uint8')   # client to which this request is assigned
    
    if (read_locs_from_trace): # need to read the DSs assigned for each unique request in the input file?
        for i in range (k_loc):
            new_trace_df['%d'%i] = trace_df['%d'%i].astype('uint8')
    return new_trace_df


def gen_requests (trace_file_name, max_num_of_req=4300000, k_loc=1, num_of_clients=1):
    """
    Generates a trace of requests, given a trace file.
    """
    if (len(trace_file_name.split ('.csv'))==2): # The input file is already a .csv parsed from the trace - no need to parse the trace
        return reduce_trace_mem_print (pd.read_csv (getTracesPath() + trace_file_name).head(max_num_of_req))
    else: # need to parse the trace first
        return parse_list_of_keys (input_file_name=trace_file_name, num_of_req = max_num_of_req, print_output_to_file=False)
    # reduce_trace_mem_print (pd.read_csv (getTracesPath() + trace_file_name).head(max_num_of_req))


def optimal_BF_size_per_DS_size ():
    """
    Sizes of the bloom filters (number of cntrs), for chosen DS sizes, k=5 hash funcs, and designed false positive rate.
    The values are taken from https://hur.st/bloomfilter
    online resource calculating the optimal values
    """
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
    Currently, traces files should be placed merely in the "/traces/" subdir, under the project's directory
    """
    return '../traces/'
#   #return 'C:/Users/' + os.getcwd().split ("\\")[2] + '/Documents/traces/' if (os.getcwd().split ("\\")[0] == "C:") else '/home/icohen/traces/'

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
    """
    An accessory function to calculate an exponential averaging window. Currently unused.
    """
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
    """
    Given an update interval, estimate the BW it would require. Currently unused.
    """
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
    if (alg_mode == ALG_OPT):
        return settings_str + 'Opt'     
    elif (alg_mode == ALG_PGM_FNO_MR1_BY_HIST):
        return settings_str + 'FNOH'
    elif (alg_mode == ALG_PGM_FNO_MR1_BY_ANALYSIS):
        return settings_str + 'FNOA'
    elif (alg_mode == ALG_PGM_FNA_MR1_BY_HIST):
        return settings_str + 'FNA'
    elif (alg_mode == ALG_PGM_FNA_MR1_BY_ANALYSIS):
        return settings_str + 'FNAA'
    elif (alg_mode == ALG_MEAURE_FP_FN):
        return settings_str + 'measure_fn'

def calc_designed_fpr (cache_size, BF_size, num_of_hashes):
    """
    returns the designed (inherent) fpr of a BF, based on the given cache size, BF size, and number of hash functions used by the BF.
    """
    return pow (1 - pow (1 - 1/BF_size, num_of_hashes * cache_size), num_of_hashes)

def parse_list_of_keys (input_file_name,
                        num_of_clients=1, # number of clients to choose from, when associating each request with a given client 
                        kloc = 1,  # number of DSs with which each unique item will be associated  
                        num_of_req = 4300000, # maximum number of requests to be considered from the trace
                        print_output_to_file=True, # When False, the func' returns the output as a dataframe, rather than printing it to a file
                        print_num_of_uniques=False # When True, print the number of unique items in the trace to the standard output
                        ):
    """
    Parses a trace whose format is merely a list of keys (each key in a different line). 
    Output: a csv file, where:
            - the first col. is the keys,
            - the 2nd col. is the id of the clients of this req,
    """

    traces_path = getTracesPath()
    df = pd.read_csv (traces_path + input_file_name, sep=' ', header=None, nrows = num_of_req)
        
    # associate each unique "url" in the input with a unique key 
    unique_urls = np.unique (df)
    url_lut_dict = dict(zip(unique_urls , range(unique_urls.size))) # generate dictionary to serve as a LUT of unique_key -> integer
    keys = np.array ([url_lut_dict[url] for url in df[0]]).astype('uint32')
    
    # generate client assignments for each request
    if (num_of_clients > 1 or print_num_of_uniques):
        client_assignment = np.random.RandomState(seed=42).randint(0 , num_of_clients , df.shape[0]).astype('uint8')
        
        unique_permutations_array  = np.array ([np.random.RandomState(seed=i).permutation(range(num_of_clients)) for i in range(unique_urls.size)]).astype('uint8') # generate permutation for each unique key in the trace
        unique_permutations_array  = unique_permutations_array [:, range (kloc)] # Select only the first kloc columns for each row (unique key) in the random permutation matrix
    
        print ('num of uniques = ', unique_permutations_array.size )
    
        permutation_lut_dict = dict(zip(unique_urls , unique_permutations_array)) # generate dictionary to serve as a LUT of unique_key -> permutation
        permutations_array = np.array([permutation_lut_dict[url] for url in df[0]]).astype('uint8') # generate full permutations array for all requests. identical keys will get identical permutations
        permutations_df = pd.DataFrame(permutations_array)
        trace_df = pd.DataFrame(np.transpose([keys, client_assignment]))
        trace_df.columns = ['key', 'client_id']
        full_trace_df = pd.concat([ trace_df, permutations_df], axis=1)
    
    else:
        full_trace_df = pd.DataFrame(np.transpose([keys]))
        full_trace_df.columns = ['key']
    
    if (print_output_to_file):    
        full_trace_df.to_csv (traces_path + input_file_name.split (".txt")[0] + '_{:.0f}K_{:.0f}DSs.csv' .format (num_of_req/1000, num_of_clients), 
                              index=False, header=True)
    else:
        return full_trace_df

    ## historgram
    #count_df = trace.value_counts()
    #count_df[count_df > 1].plot()
    #hist_array = np.zeros(int(np.log2(count_df.max())) + 1)
    #for i in range(int(np.log2(count_df.max())) + 1):
    #    hist_array[i] = sum(count_df[(count_df >= 2**i) & (count_df < 2**(i+1))])
