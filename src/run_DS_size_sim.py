import os
import numpy as np
import pandas as pd
import datetime
import sys
import pickle
from printf import printf
from   tictoc import tic, toc

import python_simulator as sim
from MyConfig import getTracesPath 
from gen_requests import gen_requests
from gen_requests import optimal_BF_size_per_DS_size
# A main file for running a sim of access strategies for different DSs (Data Stores) sizes.

num_of_DSs      = 3
num_of_clients  = num_of_DSs

max_num_of_req      = 50000 # Shorten the num of requests for debugging / shorter runs
traces_path         = getTracesPath()
# trace_file_name     = 'wiki/wiki.1190448987_50K_3DSs.csv'
# trace_file_name     = 'wiki/wiki.1190448987_800K_19DSs.csv'
# trace_file_name     = 'gradle/short.csv'
trace_file_name     = 'gradle/gradle.build-cache_50K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_400K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_50K_3DSs.csv'
requests            = pd.read_csv (traces_path + trace_file_name).head(max_num_of_req)

missp   = 100
DS_size = 4000
k_loc   = 1
bpe     = 14
max_fpr = 0.03
max_fnr = max_fpr
# alg_modes = [sim.ALG_PGM_FNO] 
alg_modes = [sim.ALG_PGM_FNA_MR1_BY_HIST] #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA, sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNA_MR1_BY_HIST_ADAPT]
num_of_events_between_updates = 300
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()
<<<<<<< HEAD
alg_modes = [sim.ALG_PGM_FNA_MR1_BY_HIST] #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA, sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNA_MR1_BY_HIST_ADAPT]
=======

output_file = open ("../res/res.txt", "a")
basic_settings_str = '{}.C{:.0f}.bpe{:.0f}.{:.0f}Kreq.{:.0f}DSs.Kloc{:.0f}.M{:.0f}.U{:.0f}.' .format \
                      (trace_file_name.split("/")[0], DS_size, bpe, max_num_of_req/1000, num_of_DSs, k_loc, missp, num_of_events_between_updates)
                
# Loop over all data store sizes, and all algorithms, and collect the data
def run_sim_collection(DS_size, missp, k_loc, requests, client_DS_cost, settings_str):
    DS_insert_mode = 1 # Currently we use only "fix" mode. See documentation in python_simulator.py

    main_sim_dict = {}
    DS_size_sim_dict = {}
    for alg_mode in alg_modes:
        if (alg_mode == sim.ALG_OPT):
            settings_str = basic_settings_str + 'Opt'
        elif (alg_mode == sim.ALG_PGM_FNO):
            settings_str = basic_settings_str + 'FNO'
        elif (alg_mode == sim.ALG_PGM_FNA):
            settings_str = basic_settings_str + 'FNA'
        elif (alg_mode == sim.ALG_PGM_FNA_MR1_BY_HIST):
            settings_str = basic_settings_str + 'FNA_by_hist'
        elif (alg_mode == sim.ALG_PGM_FNA_MR1_BY_HIST_ADAPT):
            settings_str = basic_settings_str + 'FNA_by_hist_adapt'
        print ('running', settings_str)
        tic()
        sm = sim.Simulator(output_file, settings_str, alg_mode, DS_insert_mode, requests, client_DS_cost, missp, k_loc,  
                           DS_size = DS_size, bpe = bpe, use_redundan_coef = False, max_fpr = max_fpr, max_fnr = max_fnr, 
                           verbose = 1, num_of_events_between_updates = num_of_events_between_updates)
        sm.run_simulator()
        toc()
    return main_sim_dict

# Choose parameters for running simulator    
#  load the OVH network distances and BWs
# full_path_to_rsrc   = os.getcwd() + "\\..\\resources\\"
# client_DS_dist_df   = pd.read_csv (full_path_to_rsrc + 'ovh_dist.csv',index_col=0)
# client_DS_dist      = np.array(client_DS_dist_df)
# client_DS_BW_df     = pd.read_csv (full_path_to_rsrc + 'ovh_bw.csv',index_col=0)
# client_DS_BW        = np.array(client_DS_BW_df)
# bw_regularization   = np.max(np.tril(client_DS_BW,-1)) 
# alpha = 0.5
# client_DS_cost      = 1 + alpha * client_DS_dist + (1 - alpha) * (bw_regularization / client_DS_BW) # client_DS_cost(i,j) will hold the access cost for client i accessing DS j

client_DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
client_DS_cost.fill(1)
main_sim_dict = run_sim_collection(DS_size, missp, k_loc, requests, client_DS_cost, basic_settings_str)

# client_DS_cost(i,j) will hold the access cost for client i accessing DS j
# time_str = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# sys.setrecursionlimit(50000)
# res_file = open('../res/DS_size_%d_missp_%d_kloc_%d' % (DS_size_vals[0], missp, k_loc) , 'wb')
# pickle.dump(main_sim_dict , res_file)
# res_file.close()
print ('Finished all sims')

