# # ===========================================================
# # results for variable DS_size simulations
# # ===========================================================
#
import sys
import pickle
import numpy as np
import matplotlib.pyplot as plt
import datetime
import python_simulator as sim
from printf import printf # My printf function, chomping newline in the end

ALG_OPT     = 1 # Optimal access strategy (perfect indicator)
ALG_PGM     = 2 # PGM alg', detailed in Access Strategies journal paper
ALG_CHEAP   = 3 # Cheapest (CPI) strategy: in case of a positive indication, access the minimal-cost DS with positive indication 
ALG_ALL     = 4 # All (EPI) strategy: in case of positive indications, access all DSs with positive indications
ALG_KNAP    = 5 # Knapsack-based alg'. See Access Strategies papers.
ALG_POT     = 6 # Potential-based alg'. See Access Strategies papers.
ALG_PGM_FNO = 7 # PGM alg', detailed in Access Strategies journal paper; False-Negative Oblivious
ALG_PGM_FNA = 8 # PGM alg', detailed in Access Strategies journal paper; False-Negative Aware
NUM_OF_ALGS = 8 # Number of algs'

# Currently need to specify the exact filename you would like to open and process
f = open ('../res/DS_size_1000_missp_100_kloc_1', 'rb')
full_sim_dict = pickle.load(f)
f.close()

def get_tc_ratio (full_sim_dict, DS_size, alg_num):
    return full_sim_dict [DS_size][alg_num].total_cost / full_sim_dict [DS_size][ALG_OPT].total_cost

def get_hit_ratio(full_sim_dict, DS_size, alg_num):
    return full_sim_dict[DS_size][alg_num].hit_cnt / (full_sim_dict[DS_size][alg_num].cur_req_cnt + 1)

DS_size_vals = [1000] #[200, 400, 600, 800, 1000, 1200, 1400, 1600]

# total_cost_ratio_dict = {sim.ALG_OPT: [], sim.ALG_ALL: [], sim.ALG_CHEAP: [], sim.ALG_POT: [], sim.ALG_PGM: []}
# total_cost_dict = {sim.ALG_OPT: [], sim.ALG_ALL: [], sim.ALG_CHEAP: [], sim.ALG_POT: [], sim.ALG_PGM: []}
# hit_ratio_dict = {sim.ALG_OPT: [], sim.ALG_ALL: [], sim.ALG_CHEAP: [], sim.ALG_POT: [], sim.ALG_PGM: []}

# for j, DS_size in enumerate(DS_size_vals):
#     for alg_num in [sim.ALG_OPT, sim.ALG_ALL, sim.ALG_CHEAP, sim.ALG_POT, sim.ALG_PGM]:
#         total_cost_ratio_dict[alg_num].append(float(get_total_cost(full_sim_dict,DS_size,alg_num)) / get_total_cost(full_sim_dict,DS_size,sim.ALG_OPT ))

DS_size = DS_size_vals [0]
alg_name = ['\opt', '\pgmalg', '\cpi', '\epi', '\\umb', '\pot']
# print out the results in the format required for tikz
list_of_algs = [ALG_OPT, ALG_PGM_FNO, ALG_PGM_FNA]
for alg_num in list_of_algs: #(['\opt', '\pgmalg', '\cpi', '\epi', '\\umb', '\pot'], start=1):
	if alg_num != ALG_OPT:
		if (alg_num == ALG_PGM_FNO):
			alg_name = '\pgmfno'
		elif (alg_num == ALG_PGM_FNA):
			alg_name = '\pgmfna'
		else:
			print ('unknown alg mode')
			exit ()
		printf ('& {} ' .format(alg_name))
		printf ('& {:.3f} ' .format (get_tc_ratio (full_sim_dict, DS_size, alg_num)))
		# printf ('& {:.3f} & {:.3f} ' .format (get_ac_to_opt_tc_ratio (sim_dict, missp, alg_num), get_tc_ratio (sim_dict, missp, alg_num)))
