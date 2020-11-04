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

# A main file for running simulations of Access Strategies with Staleness
num_of_DSs      = 3
num_of_clients  = num_of_DSs
DS_cost_type = 'hetro' # choose either 'homo'; 'hetro' (exponential costs - the costs are 1, 2, 4, ...); or 'ovh' (valid only if using the full 19-nodes ovh network)
max_num_of_req      = 750000 # Shorten the num of requests for debugging / shorter runs

# trace_file_name     = 'wiki/wiki.1190448987_500K_5DSs.csv'
# trace_file_name     = 'wiki/wiki.1190448987_50K_3DSs.csv'
# trace_file_name     = 'wiki/wiki.1190448987_800K_19DSs.csv'
# trace_file_name     = 'gradle/short.csv'
trace_file_name     = 'gradle/gradle.build-cache_full_750K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_400K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_50K_3DSs.csv'
requests            = pd.read_csv (getTracesPath() + trace_file_name).head(max_num_of_req)
trace_file_name     = trace_file_name.split("/")[0]
num_of_req          = requests.shape[0]
missp   = 100
DS_size = 10000
k_loc   = 1
bpe     = 14
max_fpr = 0.03
max_fnr = max_fpr
alg_modes = [sim.ALG_PGM_FNO] 
# alg_modes = [sim.ALG_PGM_FNA_MR1_BY_HIST] #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA, sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNA_MR1_BY_HIST_ADAPT]
bw = 180 # desired update bw [bits / req]
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()

output_file = open ("../res/res.txt", "a")
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

                
def run_sim_collection(DS_size, missp, k_loc, requests, DS_cost):
    
    for alg_mode in alg_modes:

        settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, alg_mode)
        print ('running', settings_str)
        tic()
        sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                           DS_size = DS_size, bpe = bpe, use_redundan_coef = False, max_fpr = max_fpr, max_fnr = max_fnr, 
                           verbose = 1, requested_bw = bw)
        sm.run_simulator()
        toc()

run_sim_collection(DS_size, missp, k_loc, requests, DS_cost)

print ('Finished all sims')

