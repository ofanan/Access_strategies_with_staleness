"""
Run a simulation, looping over all requested values of parameters 
(miss penalty, cache sizes, number of caches etc.).
"""
# A main file for running simulations of Access Strategies with Staleness

import datetime
import os
import pickle
import sys

from MyConfig import getTracesPath, settings_string, calc_service_cost_of_opt, reduce_trace_mem_print, gen_requests
import numpy as np
import pandas as pd
from printf import printf
import python_simulator as sim
from   tictoc import tic, toc

k_loc   = 1
num_of_DSs          = 3 #int (trace_file_name.split("DSs")[0].split("_")[-1]) 
num_of_clients      = num_of_DSs
DS_cost_type = 'hetro' # choose either 'homo'; 'hetro' (exponential costs - the costs are 1, 2, 4, ...); or 'ovh' (valid only if using the full 19-nodes ovh network)

bw = 0 # Use fixed given update interval, rather than calculating / estimating them based on desired BW consumption   
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()

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

def run_tbl_sim (trace_file_name):
    """
    Run a simulation with different miss penalties for the initial table
    """
    bpe                 = 14
    missp               = 100
    DS_size             = 10000
    uInterval           = 1000
    output_file         = open ("../res/tbl.res", "a")
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)

    for missp in [40, 400, 4000]:
        for alg_mode in [sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNO]:
            settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
            print ('running', settings_str)
            tic()
            sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                               DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                               verbose = 1, uInterval = uInterval)
            sm.run_simulator()
            toc()
    alg_mode = sim.ALG_OPT
    missp = 40
    settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
    print ('running', settings_str)
    tic()
    sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                       DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                       verbose = 1, uInterval = uInterval)
    sm.run_simulator()
    toc()

def run_uInterval_sim (trace_file_name):
    """
    Run a simulation where the running parameter is uInterval.
    """
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    bpe                  = 14
    missp               = 100
    DS_size             = 10000
    output_file         = open ("../res/" + trace_file_name + "_uInterval.res", "a")
    
    for alg_mode in [sim.ALG_PGM_FNO]:
        for uInterval in [8192, 4096, 2048]: #[16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]: 
            settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
            print ('running', settings_str)
            tic()
            sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                               DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                               verbose = 1, uInterval = uInterval)
            sm.run_simulator()
            toc()


def run_cache_size_sim (trace_file_name):
    """
    Run a simulation where the running parameter is cache_size.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs

    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a cache size sim' .format(num_of_req))
    bpe         = 14
    missp       = 100
    output_file = open ("../res/" + trace_file_name + "_cache_size.res", "a")
    for DS_size in [1000, 2000, 4000, 8000, 16000, 32000]:
        for uInterval in [1024]:
            for alg_mode in [sim.ALG_PGM_FNA_MR1_BY_HIST]: #[sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_OPT, sim.ALG_PGM_FNO]:
                settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
                print ('running', settings_str)
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                                   DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                                   verbose = 0, uInterval = uInterval)
                sm.run_simulator()
                toc()
            
def run_bpe_sim (trace_file_name, homo = False):
    """
    Run a simulation where the running parameter is bpe.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_size             = 10000
    missp               = 100
    output_file         = open ("../res/" + trace_file_name + "_bpe.res", "a")
       
    DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
    if (homo):
        DS_cost.fill(1)
        missp = 300 / 7 # Keep the same missp w.r.t the average a.cost as in the 3-DSs settings, with costs 1, 2, 4,
    else:
        for i in range (num_of_DSs):
            for j in range (i, i + num_of_DSs):
                DS_cost[i][j % num_of_DSs] = pow (2, j-i)
                   
    for bpe in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
        for alg_mode in [sim.ALG_PGM_FNO]: #[sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNO, sim.ALG_OPT]:            
            for uInterval in [1]:
                settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
                print ('running', settings_str)
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                                   DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                                   verbose = 1, uInterval = uInterval)
                sm.run_simulator()
                toc()
 

def run_num_of_caches_sim (trace_file_name, homo = False):
    """
    Run a simulation where the running parameter is the num of caches, and access costs are all 1.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    bw                  = 0 
    DS_size             = 10000
    bpe                 = 14
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    output_file         = open ("../res/" + trace_file_name + "_num_of_caches.res", "a")
    
    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a num of caches sim' .format(num_of_req))

    for num_of_DSs in [8, 7, 6, 5, 4, 3, 2, 1]: 
        for uInterval in [1024, 256]:
            num_of_clients      = num_of_DSs
            if (k_loc > num_of_DSs):
                print ('error: k_loc must be at most num_of_DSs')
                exit ()
             
            DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
            if (homo): 
                DS_cost.fill(1)
            else:
                for i in range (num_of_DSs):
                    for j in range (i, i + num_of_DSs):
                        DS_cost[i][j % num_of_DSs] = j-i+1
             
            missp   = 50 * np.average (DS_cost)
     
            for alg_mode in [sim.ALG_PGM_FNO, sim.ALG_PGM_FNA_MR1_BY_HIST]:
                        
                settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
                print ('running', settings_str)
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                                   DS_size = DS_size, bpe = bpe, use_redundan_coef = False, verbose = 1, 
                                   uInterval = uInterval, use_given_loc_per_item = False,)
                sm.run_simulator()
                toc()

        alg_mode = sim.ALG_OPT
                       
        settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
        print ('running', settings_str)
        tic()
        sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                           DS_size = DS_size, bpe = bpe, use_redundan_coef = False, verbose = 1, 
                           uInterval = uInterval, use_given_loc_per_item = False,)
        sm.run_simulator()
        toc()

def calc_opt_service_cost (accs_cost, comp_miss_cnt, missp, num_of_req):
    print ('Opt service cost is ', (accs_cost + comp_miss_cnt * missp) / num_of_req)

trace_file_name     = 'wiki/wiki.1190448987_4300K_3DSs.csv'
# trace_file_name     = 'gradle/gradle.build-cache_full_1000K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_1000K_3DSs.csv'
# trace_file_name     = 'umass/storage/F2.3M_req_1000K_3DSs.csv'

# run_uInterval_sim      (trace_file_name)
# run_cache_size_sim     (trace_file_name)
# run_bpe_sim              (trace_file_name, homo = False)
run_num_of_caches_sim  (trace_file_name, homo = False)


# # Opt's behavior is not depended upon parameters such as the indicaror's size, and miss penalty.
# # Hence, it suffices to run Opt only once per trace and network, and then calculate its service cost for other 
# # parameters' values. Below is the relevant auxiliary code. 
# tot_access_cost= 1018126.0
# comp_miss_cnt = 63808
# for missp in [40, 400, 4000]:
#     print ("Opt's service cost is ", calc_service_cost_of_opt (tot_access_cost, comp_miss_cnt, missp, 500000))
# exit ()

# calc_opt_service_cost (2182567, 64717, 40, 1000000)
