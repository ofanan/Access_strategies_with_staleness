# ========================================================================
# Performs sims where DS size is fixed, while missp and k_loc vary
# ========================================================================
import numpy as np
import pandas as pd
from tictoc import tic, toc
import datetime
import sys
import os
import pickle

import python_simulator as sim
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size

num_of_DSs = 19
num_of_clients = 19

## Generate the requests to be fed into the simulation. For debugging / shorter runs, pick a prefix of the trace, of length max_trace_length
max_trace_length = 1000
trace_path = 'C:/Users/ofanan/Documents/traces/wiki/'
requests = gen_requests (trace_path + 'wiki1.1190448987.csv', max_trace_length)

# load the OVH network distances and BWs
full_path_to_rsrc = os.getcwd() + "\\..\\resources\\"
client_DS_dist_df = pd.read_csv (full_path_to_rsrc + 'ovh_dist.csv',index_col=0)
client_DS_dist = np.array(client_DS_dist_df)
client_DS_BW_df = pd.read_csv (full_path_to_rsrc + 'ovh_bw.csv',index_col=0)
client_DS_BW = np.array(client_DS_BW_df)
bw_regularization = np.max(np.tril(client_DS_BW,-1))

# taken from https://hur.st/bloomfilter
# online resource calculating the optimal values
# these values are for k=5 hash functions, and a target of false positive = 0.01, 0.02, 0.05}

# Sizes of the bloom filters (number of cntrs), for chosen DS sizes, k=5 hash funcs, and designed false positive rate.
BF_size_for_DS_size = optimal_BF_size_per_DS_size ()
DS_size = 1000
uInterval = DS_size / 2;
FP_rate = 0.02
BF_size = BF_size_for_DS_size[FP_rate][DS_size]

missp = 100
def run_sim_collection(DS_size, BF_size, missp, requests, client_DS_dist, client_DS_BW, bw_regularization):
    DS_insert_mode = 1

    main_sim_dict = {}
    for k_loc in [3]: #[1, 3, 5]
        print ('k_loc = ', k_loc)
        k_loc_sim_dict = {}
        for alg_mode in [sim.ALG_OPT, sim.ALG_PGM_FNO]: #, sim.ALG_CHEAP, sim.ALG_ALL, sim.ALG_KNAP, sim.ALG_POT]:
            tic()
            sm = sim.Simulator(alg_mode, DS_insert_mode, requests, client_DS_dist, client_DS_BW, bw_regularization, missp, k_loc, DS_size = DS_size, BF_size = BF_size, uInterval = uInterval)
            sm.start_simulator()
            toc()
            k_loc_sim_dict[alg_mode] = sm
        main_sim_dict[k_loc] = k_loc_sim_dict
    return main_sim_dict
    
main_sim_dict = run_sim_collection(DS_size, BF_size, missp, requests, client_DS_dist, client_DS_BW, bw_regularization)

time_str = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

sys.setrecursionlimit(50000)
full_path_to_res = os.getcwd() + "\\..\\res\\"
res_file = open (full_path_to_res + '%s_sim_dict_missp_%d' % (time_str , missp) , 'wb') 
#pickle.dump(main_sim_dict, res_file)
res_file.close()

