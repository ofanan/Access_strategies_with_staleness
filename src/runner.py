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
bw = 0 # Use fixed given update interval, rather than calculating / estimating them based on desired BW consumption   
if (k_loc > num_of_DSs):
    print ('error: k_loc must be at most num_of_DSs')
    exit ()

def run_tbl_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation with different miss penalties for the initial table
    """
    bpe                 = 14
    missp               = 100
    DS_size             = 10000
    uInterval           = 1000
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/tbl.res", "a")
    
    for missp in [50, 100, 500]:
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

def run_uInterval_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is uInterval.
    """
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    bpe                 = 14
    missp               = 100
    DS_size             = 10000
    DS_cost            = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/" + trace_file_name + "_uInterval.res", "a")
    
    alg_mode  = sim.ALG_PGM_FNO 
    for uInterval in [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]: 
        settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
        print ('running', settings_str)
        tic()
        sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                           DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                           verbose = 1, uInterval = uInterval)
        sm.run_simulator()
        toc()

    alg_mode  = sim.ALG_PGM_FNA_MR1_BY_HIST
    for uInterval in [64, 128, 256, 512, 1024, 2048, 4096, 8192]: 
        settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
        print ('running', settings_str)
        tic()
        sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                           DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                           verbose = 1, uInterval = uInterval)
        sm.run_simulator()
        toc()

def run_cache_size_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is cache_size.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    bpe         = 14
    missp       = 100
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_cost            = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file = open ("../res/" + trace_file_name + "_cache_size.res", "a")

    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a cache size sim' .format(num_of_req))
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
                     
def calc_homo_costs (num_of_DSs):
    DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
    DS_cost.fill(2)
    return DS_cost

def calc_hetro_costs (num_of_DSs):
    DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
    for i in range (num_of_DSs):
        for j in range (i, i + num_of_DSs):
            DS_cost[i][j % num_of_DSs] = j - i + 1
    return DS_cost

def calc_DS_cost (num_of_DSs, use_homo_DS_cost = False):
    if (use_homo_DS_cost):
        return calc_homo_costs(num_of_DSs)
    else:
        return calc_hetro_costs(num_of_DSs)


def run_bpe_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is bpe.
    If the input parameter "homo" is true, the access costs are uniform 1, and the miss penalty is 300/7. 
    Else, the access costs are 1, 2, 4, and the miss penalty is 100.
    """
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = gen_requests (trace_file_name, max_num_of_req, k_loc)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_size             = 10000
    missp               = 100
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/" + trace_file_name + "_bpe.res", "a")
                       
    for bpe in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
        for uInterval in [256, 1024]:
            for alg_mode in [sim.ALG_PGM_FNA_MR1_BY_HIST, sim.ALG_PGM_FNO]:            
                settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
                print ('running', settings_str)
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                                   DS_size = DS_size, bpe = bpe, use_redundan_coef = False, 
                                   verbose = 1, uInterval = uInterval)
                sm.run_simulator()
                toc()
 

def run_num_of_caches_sim (trace_file_name, use_homo_DS_cost = True):
    """
    Run a simulation where the running parameter is the num of caches, and access costs are all 1.
    If the input parameter "homo" is true, the access costs are uniform 1, and the miss penalty is 300/7. 
    Else, the access costs are 1, 2, 4, and the miss penalty is 100.
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

    for num_of_DSs in [6]: #[1, 2, 3, 4, 5, 6, 7, 8]: 
        for uInterval in [256, 1024]:
            num_of_clients      = num_of_DSs
            if (k_loc > num_of_DSs):
                print ('error: k_loc must be at most num_of_DSs')
                exit ()
             
            DS_cost = calc_DS_cost (num_of_DSs, use_homo_DS_cost)            
            missp    = 50 * np.average (DS_cost)
     
            for alg_mode in [sim.ALG_PGM_FNA_MR1_BY_HIST]: #[sim.ALG_OPT, sim.ALG_PGM_FNO, sim.ALG_PGM_FNA_MR1_BY_HIST]:
                        
                settings_str = settings_string (trace_file_name, DS_size, bpe, num_of_req, num_of_DSs, k_loc, missp, bw, uInterval, alg_mode)
                print ('running', settings_str)
#                 print("now =", datetime.now())
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, missp, k_loc,  
                                   DS_size = DS_size, bpe = bpe, use_redundan_coef = False, verbose = 1, 
                                   uInterval = uInterval, use_given_loc_per_item = False,)
                sm.run_simulator()
                toc()


def calc_opt_service_cost (accs_cost, comp_miss_cnt, missp, num_of_req):
    print ('Opt service cost is ', (accs_cost + comp_miss_cnt * missp) / num_of_req)


trace_file_name     = 'wiki/wiki.1190448987_4300K_3DSs.csv'
# # trace_file_name     = 'gradle/gradle.build-cache_full_1000K_3DSs.csv'
# # trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_1000K_3DSs.csv'
# # trace_file_name     = 'umass/storage/F2.3M_req_1000K_3DSs.csv'

run_uInterval_sim          (trace_file_name)
# run_bpe_sim              (trace_file_name)
# run_num_of_caches_sim  (trace_file_name, use_homo_DS_cost = True)



# # Opt's behavior is not depended upon parameters such as the indicaror's size, and miss penalty.
# # Hence, it suffices to run Opt only once per trace and network, and then calculate its service cost for other 
# # parameters' values. Below is the relevant auxiliary code. 
# tot_access_cost= 1018126.0
# comp_miss_cnt = 63808
# for missp in [40, 400, 4000]:
#     print ("Opt's service cost is ", calc_service_cost_of_opt (tot_access_cost, comp_miss_cnt, missp, 500000))
