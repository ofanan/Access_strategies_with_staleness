import os
import numpy as np
import pandas as pd
import datetime
import sys
import pickle
from   tictoc import tic, toc

import python_simulator as sim
from MyConfig import getTracesPath 
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size
# A main file for running a sim of access strategies for different DSs (Data Stores) sizes.

num_of_DSs      = 3
num_of_clients  = num_of_DSs

## This produces a random matrix with a specific value on the diagonal.
## Can be used to produce a random distance matrix (with 0 on diag) and a BW matrix (with infty on diag)
#def gen_rand_matrix(num, diag_value = 0, max_dist = 30):
#    a = np.tril(np.random.randint(1,max_dist,(num,num)), k=-1)
#    return a + np.transpose(a) + np.diag(diag_value * np.ones(num))


## Generate the requests to be fed into the simulation. For debugging / shorter runs, pick a prefix of the trace, of length max_trace_length
max_num_of_req      = 300
traces_path         = getTracesPath()
input_file_name     = 'gradle/gradle.build-cache_500K_3DSs.csv'
requests            = gen_requests (traces_path + input_file_name, max_num_of_req, num_of_DSs)

missp = 100
k_loc = 1
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()
DS_size_vals = [4000] #, 400, 600, 800, 1000, 1200, 1400, 1600]

## Generate matrix for the fully homogeneous settings. This would evnetually result in client_DS_cost all 1
client_DS_dist = np.zeros((num_of_clients,num_of_DSs)) # np.ones((num_of_clients,num_of_DSs)) - np.eye(17)
client_DS_BW = np.ones((num_of_clients,num_of_DSs)) # + np.diag(np.infty * np.ones(num_of_clients))
bw_regularization = 0. # np.max(np.tril(client_DS_BW,-1))

# Sizes of the bloom filters (number of cntrs), for chosen DS sizes, k=5 hash funcs, and designed false positive rate.
BF_size_for_DS_size = optimal_BF_size_per_DS_size ()

# Loop over all data store sizes, and all algorithms, and collect the data
def run_sim_collection(DS_size_vals, missp, k_loc, requests, client_DS_cost):
    DS_insert_mode = 1

    main_sim_dict = {}
    for DS_size in DS_size_vals:
        print ('DS_size = %d' %(DS_size))
        DS_size_sim_dict = {}
        for alg_mode in [sim.ALG_PGM_FNA]: #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA]:
            tic()
            sm = sim.Simulator(alg_mode, DS_insert_mode, requests, client_DS_cost, missp, k_loc, DS_size = DS_size, bpe = 5, verbose = 0)
            sm.start_simulator()
            toc()
            DS_size_sim_dict[alg_mode] = sm
        main_sim_dict[DS_size] = DS_size_sim_dict
    return main_sim_dict

## Choose parameters for running simulator    
# load the OVH network distances and BWs
# full_path_to_rsrc   = os.getcwd() + "\\..\\resources\\"
# client_DS_dist_df   = pd.read_csv (full_path_to_rsrc + 'ovh_dist.csv',index_col=0)
# client_DS_dist      = np.array(client_DS_dist_df)
# client_DS_BW_df     = pd.read_csv (full_path_to_rsrc + 'ovh_bw.csv',index_col=0)
# client_DS_BW        = np.array(client_DS_BW_df)
# bw_regularization   = np.max(np.tril(client_DS_BW,-1)) 
# alpha = 0.5
# client_DS_cost      = 1 + alpha * client_DS_dist + (1 - alpha) * (bw_regularization / client_DS_BW) # client_DS_cost(i,j) will hold the access cost for client i accessing DS j
#client_DS_cost = np.array ([ [1,2,3,4], [5,6,7,9],[8,12,15, 17], [2,2,5,8]])
client_DS_cost = np.empty (shape=(num_of_DSs,num_of_DSs))
client_DS_cost.fill(1)
main_sim_dict = run_sim_collection(DS_size_vals, missp, k_loc, requests, client_DS_cost)

# client_DS_cost(i,j) will hold the access cost for client i accessing DS j
# time_str = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# sys.setrecursionlimit(50000)
# res_file = open('../res/DS_size_%d_missp_%d_kloc_%d' % (DS_size_vals[0], missp, k_loc) , 'wb')
# pickle.dump(main_sim_dict , res_file)
# res_file.close()
print ('Finished all sims')

