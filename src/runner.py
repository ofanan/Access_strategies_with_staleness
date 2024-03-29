"""
Run a simulation, looping over all requested values of parameters 
(miss penalty, cache sizes, number of caches etc.).
"""

from datetime import datetime
import os, pickle, sys

import MyConfig
import numpy as np
import pandas as pd
from printf import printf
import python_simulator as sim
from   tictoc import tic, toc
import settings

def calc_homo_costs (num_of_DSs, num_of_clients):
    """
    Returns a DS_cost matrix, representing an homogeneous access cost of 2 from each client to each DS. 
    """
    DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
    DS_cost.fill(2)
    return DS_cost

def calc_hetro_costs (num_of_DSs, num_of_clients):
    """
    Returns a DS_cost matrix, where the access cost from client i to DS j is j-i+1  
    """
    DS_cost = np.empty (shape=(num_of_clients,num_of_DSs))
    for i in range (num_of_clients):
        for j in range (i, i + num_of_DSs):
            DS_cost[i][j % num_of_DSs] = j - i + 1
    return DS_cost

def calc_DS_cost (num_of_DSs = 3, use_homo_DS_cost = False):
    """
    Returns a DS_cost matrix - either homo' or hetro', by the user's request  
    """
    num_of_clients      = 1 #num_of_DSs
    if (use_homo_DS_cost):
        return calc_homo_costs(num_of_DSs, num_of_clients)
    else:
        return calc_hetro_costs(num_of_DSs, num_of_clients)

def run_var_missp_sim (trace_file_name, use_homo_DS_cost = False, print_est_mr=True, print_real_mr=False, max_num_of_req=1000000):
    """
    Run a simulation with different miss penalties for the initial table
    """
    num_of_DSs          = 3
    uInterval           = 1000
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req) # Generate a dataframe of requests from the input trace file
    num_of_req          = requests.shape[0]
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/tbl.res", "a")
    # est_mr_output_file  = open (('../res/{}_est_mr.res' .format (trace_file_name.split ('/')[1].split('.csv')[0])), 'w') if (print_est_mr)  else None
    # real_mr_output_file = 1 if (print_real_mr) else None
    
    print("now = ", datetime.now(), 'running var_missp sim')
    for missp in [100]: #50, 100, 500]:
        for alg_mode in [MyConfig.ALG_PGM_FNA_MR1_BY_ANALYSIS]: #ALG_PGM_FNA_MR1_BY_ANALYSIS
            tic()
            sm = sim.Simulator(output_file, trace_file_name.split("/")[0], 
                               alg_mode, requests, DS_cost, 
                               uInterval = uInterval, missp = missp,
                               print_est_vs_real_mr = True,
                               DS_size = 10000, verbose=0)
            sm.run_simulator(interval_between_mid_reports=100000)
            toc()

def run_uInterval_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is uInterval.
    """
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    num_of_DSs          = 3
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/" + trace_file_name + "_uInterval.res", "a")
    
    print("now = ", datetime.now(), 'running uInterval sim')
    for alg_mode in [MyConfig.ALG_PGM_FNA_MR1_BY_ANALYSIS]:  
        for uInterval in [8192, 4096, 2048, 1024, 512, 256, 128, 64, 32, 16]:
            if (alg_mode == MyConfig.ALG_PGM_FNA_MR1_BY_ANALYSIS and uInterval < 50): # When uInterval < parameters updates interval, FNO and FNA are identical, so no need to run also FNA
                continue
            tic()
            sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, uInterval = uInterval)        
            sm.run_simulator()
            toc()

def run_cache_size_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is cache_size.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    num_of_DSs          = 3
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file = open ("../res/" + trace_file_name + "_cache_size.res", "a")

    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a cache size sim' .format(num_of_req))
    for DS_size in [1000, 2000, 4000, 8000, 16000, 32000]:
        for uInterval in [1024, 256]:
            for alg_mode in [MyConfig.ALG_PGM_FNO_MR1_BY_ANALYSIS]: #[MyConfig.ALG_PGM_FNA_MR1_BY_HIST, MyConfig.ALG_OPT, MyConfig.ALG_PGM_FNO_MR1_BY_HIST]:
                print("now = ", datetime.now(), 'running cache_size sim')
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, uInterval = uInterval, DS_size = DS_size)
                sm.run_simulator()
                toc()
                     
def run_bpe_sim (trace_file_name, use_homo_DS_cost = False):
    """
    Run a simulation where the running parameter is bpe.
    If the input parameter "homo" is true, the access costs are uniform 1, and the miss penalty is 300/7. 
    Else, the access costs are 1, 2, 4, and the miss penalty is 100.
    """
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    num_of_DSs          = 3
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    DS_cost             = calc_DS_cost (num_of_DSs, use_homo_DS_cost)
    output_file         = open ("../res/" + trace_file_name + "_bpe.res", "a")
                       
    print("now = ", datetime.now(), 'running bpe sim')
    for bpe in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
        for uInterval in [1024, 256]:
            for alg_mode in [MyConfig.ALG_PGM_FNO_MR1_BY_ANALYSIS]: #MyConfig.ALG_PGM_FNO_MR1_BY_HIST]: #MyConfig.ALG_PGM_FNO_MR1_BY_ANALYSIS             
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, bpe = bpe, uInterval = uInterval) 
                sm.run_simulator()
                toc()
 

def run_num_of_caches_sim (trace_file_name, use_homo_DS_cost = True):
    """
    Run a simulation where the running parameter is the num of caches, and access costs are all 1.
    If the input parameter "homo" is true, the access costs are uniform 1, and the miss penalty is 300/7. 
    Else, the access costs are 1, 2, 4, and the miss penalty is 100.
    """
    DS_size             = 10000
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req)
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    output_file         = open ("../res/" + trace_file_name + "_num_of_caches.res", "a")
    
    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a num of caches sim' .format(num_of_req))

    for num_of_DSs in [1, 2, 3, 4, 5, 6, 7, 8]: 
        for uInterval in [1024]:
            DS_cost = calc_DS_cost (num_of_DSs, use_homo_DS_cost)            
            missp    = 50 * np.average (DS_cost)
     
            for alg_mode in [MyConfig.ALG_PGM_FNO_MR1_BY_ANALYSIS]: #[MyConfig.ALG_OPT, MyConfig.ALG_PGM_FNO_MR1_BY_HIST, MyConfig.ALG_PGM_FNA_MR1_BY_HIST]:
                        
                print("now = ", datetime.now(), 'running num of caches sim')
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, uInterval = uInterval)
                sm.run_simulator()
                toc()


def run_k_loc_sim (trace_file_name, use_homo_DS_cost = True):
    """
    Run a simulation where the running parameter is the num of caches, and access costs are all 1.
    If the input parameter "homo" is true, the access costs are uniform 1, and the miss penalty is 300/7. 
    Else, the access costs are 1, 2, 4, and the miss penalty is 100.
    """
    max_num_of_req      = 4300000 # Shorten the num of requests for debugging / shorter runs
    k_loc               = 1
    num_of_DSs          = 8
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req, k_loc) # In this sim', each item's location will be calculated as a hash of the key. Hence we actually don't use the k_loc pre-computed entries. 
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    output_file         = open ("../res/" + trace_file_name + "_k_loc.res", "a")
    
    if (num_of_req < 4300000):
        print ('Note: you used only {} requests for a num of caches sim' .format(num_of_req))

    for k_loc in [3]:
        for uInterval in [256]:
    
            DS_cost = calc_DS_cost (num_of_DSs, use_homo_DS_cost)            
            missp    = 50 * np.average (DS_cost)
     
#             for alg_mode in [MyConfig.ALG_PGM_FNA_MR1_BY_ANALYSIS]: 
            # for alg_mode in [MyConfig.ALG_PGM_FNO_MR1_BY_HIST]: 
            for alg_mode in [MyConfig.ALG_OPT]: 
                        
                print("now = ", datetime.now(), 'running k_loc sim')
                tic()
                sm = sim.Simulator(output_file, trace_file_name, alg_mode, requests, DS_cost, uInterval = uInterval, k_loc = k_loc)
                sm.run_simulator()
                toc()

def run_FN_by_staleness_sim (): 
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    DS_cost             = calc_DS_cost ()            
    output_file         = open ("../res/FN_by_staleness.res", "a")
    print("now = ", datetime.now(), 'running FN_by_staleness sim')

    for trace_file_name in ['scarab/scarab.recs.trace.20160808T073231Z.15M_req_1000K_3DSs.csv', 'umass/storage/F2.3M_req_1000K_3DSs.csv']:
        requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req)  
        trace_file_name     = trace_file_name.split("/")[0]
        num_of_req          = requests.shape[0]
        printf (output_file, '\n\ntrace = {}\n///////////////////\n' .format (trace_file_name))
    
        for bpe in [2, 4, 8, 16]:
            tic()
            sm = sim.Simulator(output_file, trace_file_name, MyConfig.ALG_PGM_FNO_MR1_BY_HIST, requests, DS_cost, bpe = bpe,    
                               verbose = sim.CNT_FN_BY_STALENESS, uInterval = 8192) # In this sim', each item's location will be calculated as a hash of the key. Hence we actually don't use the k_loc pre-computed entries.
            sm.run_simulator()
            toc()


def run_FN_by_uInterval_sim (trace_file_name): 
    max_num_of_req      = 1000000 # Shorten the num of requests for debugging / shorter runs
    requests            = MyConfig.gen_requests (trace_file_name, max_num_of_req) # In this sim', each item's location will be calculated as a hash of the key. Hence we actually don't use the k_loc pre-computed entries. 
    DS_cost             = calc_DS_cost(num_of_DSs=1)            
    trace_file_name     = trace_file_name.split("/")[0]
    num_of_req          = requests.shape[0]
    
    print("now = ", datetime.now(), 'running FN_by_uInterval_sim sim')
    for bpe in [4, 8, 16]:
        output_file = open ("../res/" + trace_file_name + "_FN_by_uInterval_bpe" + str(bpe) +".res", "a")

        for uInterval in [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]:
            tic()
            sm = sim.Simulator(output_file, trace_file_name, MyConfig.ALG_MEAURE_FP_FN, requests, DS_cost,    
                               verbose = 0, bpe = bpe, uInterval = uInterval)
            sm.run_simulator()
            toc()


def calc_opt_service_cost (accs_cost, comp_miss08_cnt, missp, num_of_req):
    print ('Opt service cost is ', (accs_cost + comp_miss_cnt * missp) / num_of_req)


# trace_file_name = 'wiki/wiki1.1190448987.txt'
# trace_file_name = 'gradle/gradle.build-cache.xz.txt'
# trace_file_name = 'scarab/scarab.recs.trace.20160808T073231Z.xz.txt'

# trace_file_name       = 'wiki/wiki.1190448987_4300K_3DSs.csv'
# trace_file_name     = 'gradle/gradle.build-cache_full_1000K_3DSs.csv'
# trace_file_name     = 'scarab/scarab.recs.trace.20160808T073231Z.15M_req_1000K_3DSs.csv'
# trace_file_name     = 'umass/storage/F2.3M_req_1000K_3DSs.csv'
run_var_missp_sim(trace_file_name = 'gradle/gradle.build-cache.xz.txt', max_num_of_req=500)
# print (MyConfig.gen_requests ('wiki/wiki2.1191403252.gz.txt', max_num_of_req=5))

# run_FN_by_uInterval_sim (trace_file_name)

# run_FN_by_staleness_sim ()
# run_bpe_sim              (trace_file_name)
# run_uInterval_sim(trace_file_name)

# run_cache_size_sim(trace_file_name)
# run_num_of_caches_sim  (trace_file_name, use_homo_DS_cost = True)
# run_k_loc_sim (trace_file_name)


# # Opt's behavior is not depended upon parameters such as the indicaror's size, and miss penalty.
# # Hence, it suffices to run Opt only once per trace and network, and then calculate its service cost for other 
# # parameters' values. Below is the relevant auxiliary code. 
# tot_access_cost= 1805450.0
# comp_miss_cnt = 104710
# num_of_req = 1000000
# for missp in [50, 500]:
#     print ("Opt's service cost is ", MyConfig.calc_service_cost_of_opt (tot_access_cost, comp_miss_cnt, missp, num_of_req))
