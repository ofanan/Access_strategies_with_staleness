# ========================================================================
# Parse results for sim where DS size is fixed, while missp and k_loc vary
# ========================================================================

import sys
import pickle
import numpy as np
import matplotlib.pyplot as plt
import python_simulator as sim
import os
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

def get_ac_to_opt_tc_ratio (sim_dict, kloc, missp, alg_num):
    return sim_dict[missp][kloc][alg_num].total_access_cost / sim_dict[missp][kloc][sim.ALG_OPT].total_cost

def get_tc_ratio (sim_dict, kloc, missp, alg_num):
    return sim_dict[missp][kloc][alg_num].total_cost / sim_dict[missp][kloc][sim.ALG_OPT].total_cost


full_path = "../res/" #os.getcwd() + 
filename = full_path + "2020_08_31_18_10_09_sim_dict_missp_100"

sim_dict = {}
f = open(filename, 'rb')
missp = int(filename.split ('_')[-1])
sim_dict[missp] = pickle.load(f)
f.close()
print ('missp = ', missp)

list_of_algs = [ALG_OPT, ALG_PGM_FNO]
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
		for k in sim_dict[missp]:
			printf ('& {:.3f} & {:.3f} ' .format (get_ac_to_opt_tc_ratio (sim_dict, k, missp, alg_num), get_tc_ratio (sim_dict, k, missp, alg_num)))


# for alg_num, alg_name in enumerate (['\opt', '\pgmalg'], start=1): #(['\opt', '\pgmalg', '\cpi', '\epi', '\\umb', '\pot'], start=1):
# 	if alg_name not in (['\opt']):
# 		print('& {} & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} bb'.format(alg_name, \
# 			get_ac_to_opt_tc_ratio (sim_dict, 1, missp, alg_num), \
# 			get_tc_ratio (sim_dict, 1, missp, alg_num), \
# 			get_ac_to_opt_tc_ratio(sim_dict, 3, missp, alg_num), \
# 			get_tc_ratio(sim_dict, 3, missp, alg_num), \
# 			get_ac_to_opt_tc_ratio (sim_dict, 5, missp, alg_num), \
# 			get_tc_ratio (sim_dict, 5, missp, alg_num)))

#		print('& {} & {:.2f} & {:.2f} '.format(alg_name, \
#			get_ac_to_opt_tc_ratio (sim_dict, 3, missp, alg_num), \
#			get_tc_ratio (sim_dict, 3, missp, alg_num)))
