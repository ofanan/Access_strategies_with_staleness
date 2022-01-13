"""
This script calculates the cost values for the fully-homogeneous case; and generates a heatmap.
The cost depends upon Phit, missp, number of caches, false-positive-rate (fpr), and false-negative-rate (fnr).
The heatmap presents the cost, for given missp and number of functions, where the x,y axes are fpr, fnr.
"""

import math
import numpy as np, pandas as pd
from scipy import special
from scipy.stats import binom

# Calculate the cost func' (PHI)
def cost (k0, k1, rho0, rho1, missp):
    return k0 + k1 + missp * np.power (rho0, k0) * np.power (rho1, k1)

# calculate probability_of_positive_ind:
# probability_of_positive_ind (i) will hold Pr(I(x)=1), given that the hit rate is Phit(i)
def calc_prob_of_positive_ind (fpr, fnr):
    return Phit * (1 - fnr) + Pmiss * fpr # Probability of having a positive ind' in an indicator

# calculate rho1:
# rho1(i) is Pr(x \notin S | I(x)=1), when the hit rate is Phit(i)
def calc_rho1 (fpr, fnr, q):
    rho1 = np.ones (NUM_OF_VALS) #default value
    rho1 = fpr * Pmiss  / q
    return rho1

# calculate rho0:
# rho0(i) is Pr(x \notin S | I(x)= 0), when the hit rate is Phit(i)
def calc_rho0 (fpr, fnr, q):
    rho0 = np.ones (NUM_OF_VALS) #default value
    rho0 = (1 - fpr) * Pmiss  / (1 - q )
    return rho0

# Calculate k^*, namely, the # of $s to accs - for the cases of PI, II, and II_FNO.
def calc_k_opt (rho, max_k, cur_missp):

    # By default, for all i we have k_opt[i]=0, and the minimal found cost is cur_missp[i]
    k_opt = np.zeros(NUM_OF_VALS)
    # Loop over all Phit values

    for i in range (NUM_OF_VALS):
        min_cost = cur_missp[i]
        for k in range (1, max_k+1):
            if hasattr(rho, "__len__"):
                rho_i = rho[i]
            else:
                rho_i = rho
            suggested_cost = k + cur_missp[i] * np.power (rho_i, k)
            # Patch for the case rho is a scalar, not a vector
            if (suggested_cost < min_cost): # Found a better value of k
                k_opt[i], min_cost = k, suggested_cost

    return k_opt


# Calculate k^*, namely, the optimal # of $s to accs - for the conf' of NI
def calc_k_opt_NI (rho, max_k, cur_missp):

    # By default, k_opt=0, and the minimal found cost is cur_missp
    k_opt, min_cost = 0, cur_missp
    # When the X axis is not Phit, k used by NI is constant - no need to iterate
    for k in range (1, max_k+1):
        suggested_cost = k + cur_missp * np.power (rho, k)
        if (suggested_cost < min_cost): # Found a better value of k
            k_opt, min_cost = k, suggested_cost

    return k_opt

# Calc the policy of Expected Cost Minimization,
# Assuming Only False Positives (no false negatives).
def ECM_FNO (rho1, num_of_pos_inds):
    # Calculate k1^*, namely, the # of $s w/ pos ind to accs.
    # Assume here we accs no $s w/ neg. ind.
    k1_opt = calc_k_opt (rho1, num_of_pos_inds, missp * np.ones((NUM_OF_VALS)))
    cost = k1_opt + missp * np.power (rho1, k1_opt) # Access only $s w/ pos. ind. (if at all)
    return cost

# Calc the (expected) cost of the Expected Cost Minimization policy
def ECM (rho0, rho1, num_of_pos_inds):
    # Calculate k1^*, namely, the # of $s w/ pos ind to accs.
    # Assume here we accs no $s w/ neg. ind.
    k1_opt = calc_k_opt (rho1, num_of_pos_inds, missp * np.ones((NUM_OF_VALS)))

    # Calculate k0^*, namely, the # of $s w/ pos ind to accs.
	# Assume here we accs k1 $s w/ neg. ind,
	# so the current miss penalty is missp * np.power (rho1, k1)
    k0_opt = calc_k_opt (rho0, n - num_of_pos_inds, missp * np.power (rho1, k1_opt))
    # print ('k1_opt = ', k1_opt, 'k0_opt = ', k0_opt)
    return cost (k0_opt, k1_opt, rho0, rho1, missp)

# Calculate the cost of a random indicator
def calc_no_indicator (n):
    fpr, fnr = 1, 0     # False Positive Rate, False Negative Rate
    # in the lack of an indicator, rho0, rho1 are both the miss ratio
    rho0, rho1 = Pmiss, Pmiss
    num_of_pos_inds = n
    k1 = calc_k_opt_NI (rho1, num_of_pos_inds, missp)
    k0 = 0
    return cost (k0, k1, rho0, rho1, missp) * np.ones (NUM_OF_VALS)


# Calculate the cost of a perfect indicator
def calc_perfect_indicator (n):
    return 1 + (missp-1) * np.power(Pmiss, n)

# Calculate the costs of a perfect indicator, which assumes no false neg. (Only False Positives)
# In this version of the func, rho1 is also calculated assuming no FN, and hence the estimated rho1 is inaccurate
def calc_imperfect_indicator_FNO_rho_by_FNO (n):
    C_II_FNO =  np.zeros (NUM_OF_VALS)
    # C_II_FNO += indicator_cost
    real_q = calc_prob_of_positive_ind(fpr, fnr) # real prob' of having a pos ind'
    estimated_q = calc_prob_of_positive_ind(fpr, 0) # prob' of having a pos ind' as estimated by OFP policy
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, real_q)
        # Note that rho1 is calculated assuming fnr=0
        C_II_FNO += pdf_of_num_of_pos_inds * ECM_FNO (calc_rho1 (fpr, 0, estimated_q), num_of_pos_inds)
    return C_II_FNO

# Calculate the costs of a perfect indicator, which assumes no false neg. (Only False Positives)
# In this version of the func, rho1 is evaluated directly (as done in accs strategies), and hence is accurate
def calc_imperfect_indicator_FNO (n, fpr, fnr):
    C_II_FNO =  np.zeros (NUM_OF_VALS)
    # C_II_FNO += indicator_cost
    real_q = calc_prob_of_positive_ind(fpr, fnr) # real prob' of having a pos ind'
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, real_q)
        # Note that rho1 is calculated assuming fnr=0
        C_II_FNO += pdf_of_num_of_pos_inds * ECM_FNO (calc_rho1 (fpr, fnr, real_q), num_of_pos_inds)
    return C_II_FNO

# Calculate the costs of a perfect indicator
def calc_imperfect_indicator (n, fpr, fnr):
    C_II =  np.zeros (NUM_OF_VALS)
    # C_II += indicator_cost
    q = calc_prob_of_positive_ind(fpr, fnr)
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, q)
        C_II += pdf_of_num_of_pos_inds * ECM (calc_rho0 (fpr, fnr, q), calc_rho1 (fpr, fnr, q), num_of_pos_inds)
    return C_II

if __name__ == "__main__":

# ########################################################################################################
# main for heatmap where the X, Y axes are n, Phit
# ########################################################################################################
# missp = 100;
# max_n = 10
# NUM_OF_VALS=10
# Phit =
# Phit = np.array (range(1, NUM_OF_VALS) ).astype('float') / NUM_OF_VALS
#
# # Calc the cost of NI
# # C_NI[n] will hold the service cost of NI when there're n caches.
# C_NI = np.zeros (max_n+1)
# for n in range (1, max_n+1):
#     for Phit_idx in range (NUM_OF_VALS)
#         cur_Phit = Phit[Phit_idx]
#         Pmiss = 1 - cur_Phit
#         print('%d %.4f %.4f' % (n, Phit[Phit_idx], calc_perfect_indicator(n), calc_perfect_indicator(n))
#     print ('')


    missp           = 3000000
    n               = 10010
    num_of_pos_inds = 10
    rho0            = 0.4999
    rho1            = 0.4998999999999
    his_k0          = 16.99972905
    his_k1          = 3.9822355
    
    his_sol_cost     = cost(k0=his_k0, k1=his_k1, rho0=rho0, rho1=rho1, missp=missp)
    his_int_sol_cost = min (cost(k0=math.floor(his_k0), k1=math.floor(his_k1), rho0=rho0, rho1=rho1, missp=missp),
                            cost(k0=math.floor(his_k0), k1=math.ceil (his_k1), rho0=rho0, rho1=rho1, missp=missp),
                            cost(k0=math.ceil (his_k0), k1=math.floor(his_k1), rho0=rho0, rho1=rho1, missp=missp),
                            cost(k0=math.ceil (his_k0), k1=math.ceil (his_k1), rho0=rho0, rho1=rho1, missp=missp))

    print ('his sol cost={}'     .format (his_sol_cost))
    print ('his int sol cost={}' .format (cost(k0=round(his_k0), k1=round(his_k1), rho0=rho0, rho1=rho1, missp=missp)))

    print ('our cost by him={}' .format (cost(k0=11, k1=10, rho0=rho0, rho1=rho1, missp=missp)))
    
    min_cost = float ('inf')
    for k1 in range (num_of_pos_inds+1):
        k1_cost = cost(k0=0, k1=k1, rho0=rho0, rho1=rho1, missp=missp) 
        if (k1_cost < min_cost):
            min_cost = k1_cost
            k1_opt   = k1
    print ('k1_opt = {}' .format (k1_opt))   

    our_sol_cost = float ('inf')
    for k0 in range (n-num_of_pos_inds+1):
        k0_cost = cost(k0=k0, k1=k1_opt, rho0=rho0, rho1=rho1, missp=missp) 
        if (k0_cost < our_sol_cost):
            our_sol_cost = k0_cost
            k0_opt       = k0
    print ('k0_opt = {}' .format (k0_opt))
    print ('our sol: k0_opt={}, k1_opt={}, cost={}' .format (k0_opt, k1_opt, our_sol_cost))    
    
    opt_int_sol_cost = float ('inf')
    for k1 in range (num_of_pos_inds+1):
        for k0 in range (n-num_of_pos_inds+1):
            k0_k1_cost = cost(k0=k0, k1=k1, rho0=rho0, rho1=rho1, missp=missp)
            if (k0_k1_cost < opt_int_sol_cost):
                opt_int_sol_cost = k0_k1_cost
                k0_opt, k1_opt = k0, k1 
    
    print ('opt int sol: k0_opt={}, k1_opt={}, cost={}' .format (k0_opt, k1_opt, opt_int_sol_cost))    
    
    print ('opt_int_sol_cost - our_sol_cost=', opt_int_sol_cost - our_sol_cost)

    print ('his_int_sol_cost - our_sol_cost=', his_int_sol_cost - our_sol_cost)
# print ('ECM cost=')
    
    
