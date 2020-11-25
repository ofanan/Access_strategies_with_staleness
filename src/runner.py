import os
import numpy as np
import pandas as pd
import datetime
import sys
import pickle
from printf import printf
from   tictoc import tic, toc

import python_simulator as sim
from MyConfig import getTracesPath, settings_string 
from gen_requests import gen_requests, optimal_BF_size_per_DS_size

"""
Run a simulation, looping over all requested values of parameters 
(miss penalty, cache sizes, number of caches etc.).
"""

# A main file for running simulations of Access Strategies with Staleness
DS_cost_type = 'hetro' # choose either 'homo'; 'hetro' (exponential costs - the costs are 1, 2, 4, ...); or 'ovh' (valid only if using the full 19-nodes ovh network)
max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs

trace_file_name     = 'wiki/wiki.1190448987_1000K_3DSs.csv'
# trace_file_name     = 'wiki/wiki.1190448987_800K_19DSs.csv'
# trace_file_name     = 'gradle/gradle.build-cache_full_750K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_1000K_3DSs.csv'
# trace_file_name     = 'umass/storage/F2.3M_req_500K_3DSs.csv'

num_of_DSs      = int (trace_file_name.split("DSs")[0].split("_")[-1]) 
num_of_clients  = num_of_DSs
requests            = pd.read_csv (getTracesPath() + trace_file_name).head(max_num_of_req)
trace_file_name     = trace_file_name.split("/")[0]
num_of_req          = requests.shape[0]

miss_penalties = [100]
k_loc   = 1
max_fpr = 0.03
max_fnr = max_fpr 
alg_modes = [sim.ALG_PGM_FNO, sim.ALG_PGM_FNA_MR1_BY_HIST] 
# alg_modes = [sim.ALG_PGM_FNA_MR1_BY_HIST] #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA, sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNA_MR1_BY_HIST_ADAPT]

bw = 0 # Use fixed given update interval, rather than calculating / estimating them based on desired BW consumption   
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()

output_file = open ("../res/" + trace_file_name + ".res", "a")
DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
if (DS_cost_type == 'homo'):
    DS_cost.fill(1)
elif (DS_cost_type == 'hetro'):
    for i in range (num_of_DSs):
        for j in range (i, i + num_of_DSs):
            DS_cost[i][j % num_of_DSs] = pow (2, j-i)
elif (DS_cost_type == 'ovh'):
    if (num_of_DSs != 19):
        print ('error: you asked to run OVH costs, but num of DSs is not 19')
        exit ()
    DS_cost = calcOvhDsCost ()
else: 
    print ('The DS_cost type you chose is not supported')

# Cache size sim'                
bpe         = 14
DS_sizes    = [1000, 2000, 4000, 8000, 16000, 32000]
missp       = 100
def run_sim_collection (k_loc, requests, DS_cost):
    
    uInterval   = 1024
    for DS_size in DS_sizes:
        for alg_mode in alg_modes:
    
            settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
            print ('running', settings_str)
            tic()
            sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                               DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                               verbose = 1, uInterval = uInterval)
            sm.run_simulator()
            toc()
            
# # bpe sim'                
# DS_size     = 10000
# uInterval   = DS_size / 10
# missp       = 100
# bpes        = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
# def run_sim_collection (k_loc, requests, DS_cost):
#     
#     for bpe in bpes:
#         for alg_mode in alg_modes:
#     
#             settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
#             print ('running', settings_str)
#             tic()
#             sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
#                                DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
#                                verbose = 1, uInterval = uInterval)
#             sm.run_simulator()
#             toc()

run_sim_collection (k_loc, requests, DS_cost)

print ('Finished all sims')

