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

max_num_of_req      = 12000# 50000 #0 # Shorten the num of requests for debugging / shorter runs
traces_path         = getTracesPath()
input_file_name     = 'wiki/wiki1.1190448987_50K.3DSs.K3.csv'
requests            = gen_requests (traces_path + input_file_name, max_num_of_req, num_of_DSs)

missp = 100
k_loc = 1
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()
DS_size_vals = [4000] 
alg_modes = [sim.ALG_PGM_FNA] #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA]

# Loop over all data store sizes, and all algorithms, and collect the data
def run_sim_collection(DS_size_vals, missp, k_loc, requests, client_DS_cost):
    DS_insert_mode = 1 # Currently we use only "fix" mode. See documentation in python_simulator.py

    main_sim_dict = {}
    for DS_size in DS_size_vals:
        print ('DS_size = %d, missp = %d, ' %(DS_size, missp))
        DS_size_sim_dict = {}
        for alg_mode in alg_modes:
            tic()
            sm = sim.Simulator(alg_mode, DS_insert_mode, requests, client_DS_cost, missp, k_loc, DS_size = DS_size, bpe = 5, 
                                use_redundan_coef = False, use_adaptive_alg = False, verbose = 3)
            sm.run_simulator()
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
client_DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
client_DS_cost.fill(1)
main_sim_dict = run_sim_collection(DS_size_vals, missp, k_loc, requests, client_DS_cost)
# indications = np.array (range (2), dtype = 'bool') 
# indications = [True, False]
# print ('indications = ', indications)

# client_DS_cost(i,j) will hold the access cost for client i accessing DS j
# time_str = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# sys.setrecursionlimit(50000)
# res_file = open('../res/DS_size_%d_missp_%d_kloc_%d' % (DS_size_vals[0], missp, k_loc) , 'wb')
# pickle.dump(main_sim_dict , res_file)
# res_file.close()
print ('Finished all sims')

