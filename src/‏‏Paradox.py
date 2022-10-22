import numpy as np
import numpy as np
import pandas as pd
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
    rho1 = np.ones (len (q)) #default value
    rho1 = fpr * Pmiss  / q
    return rho1

# calculate rho0:
# rho0(i) is Pr(x \notin S | I(x)= 0), when the hit rate is Phit(i)
def calc_rho0 (fpr, fnr, q):
    rho0 = np.ones (NUM_OF_VALS) #default value
    rho0 = (1 - fpr) * Pmiss  / (1 - q )
    return rho0

# Calculate k^*, namely, the # of $s to accs - for the cases of PI, ECM_FNA, and ECM_FNO.
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


# Calculate k^*, namely, the # of $s to accs - for the conf' of NI (No Indicator)
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
# Considering false negatives, namely, FNA (False Negative Aware)
def ECM_FNA (rho0, rho1, num_of_pos_inds):
    # Calculate k1^*, namely, the # of $s w/ pos ind to accs.
    # Assume here we accs no $s w/ neg. ind.
    k1_opt = calc_k_opt (rho1, num_of_pos_inds, missp * np.ones((NUM_OF_VALS)))

    # Calculate k0^*, namely, the # of $s w/ pos ind to accs.
	# Assume here we accs k1 $s w/ neg. ind,
	# so the current miss penalty is missp * np.power (rho1, k1)
    k0_opt = calc_k_opt (rho0, n - num_of_pos_inds, missp * np.power (rho1, k1_opt))
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
def cost_of_perfect_indicator (n):
    return 1 + (missp-1) * np.power(Pmiss, n)

# Calculate the costs of a perfect indicator, which assumes no false neg. (Only False Positives)
# In this version of the func, rho1 is also calculated assuming no FN, and hence the estimated rho1 is inaccurate
def cost_of_imperfect_indicator_FNO_rho_by_FNO (n):
    cost_ECM_FNO =  np.zeros (NUM_OF_VALS)
    # cost_ECM_FNO += indicator_cost
    real_q = calc_prob_of_positive_ind(fpr, fnr) # real prob' of having a pos ind'
    estimated_q = calc_prob_of_positive_ind(fpr, 0) # prob' of having a pos ind' as estimated by OFP policy
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, real_q)
        # Note that rho1 is calculated assuming fnr=0
        cost_ECM_FNO += pdf_of_num_of_pos_inds * ECM_FNO (calc_rho1 (fpr, 0, estimated_q), num_of_pos_inds)
    return cost_ECM_FNO

# Calculate the costs of a perfect indicator, which assumes no false neg. (Only False Positives)
# In this version of the func, rho1 is evaluated directly (as done in accs strategies), and hence is accurate
def cost_of_imperfect_indicator_FNO (n, fpr, fnr):
    cost_ECM_FNO =  np.zeros (NUM_OF_VALS)
    # cost_ECM_FNO += indicator_cost
    real_q = calc_prob_of_positive_ind(fpr, fnr) # real prob' of having a pos ind'
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, real_q)
													 
        cost_ECM_FNO += pdf_of_num_of_pos_inds * ECM_FNO (calc_rho1 (fpr, fnr, real_q), num_of_pos_inds)
    return cost_ECM_FNO

# Calculate the costs of an imperfect indicator
def cost_of_imperfect_indicator_FNA (n, fpr, fnr):
    cost_ECM_FNA =  np.zeros (NUM_OF_VALS)
    # cost_ECM_FNA += indicator_cost
    q = calc_prob_of_positive_ind(fpr, fnr)
    for num_of_pos_inds in range (0, n+1):
        pdf_of_num_of_pos_inds = binom.pmf (num_of_pos_inds, n, q)
        cost_ECM_FNA += pdf_of_num_of_pos_inds * ECM_FNA (calc_rho0 (fpr, fnr, q), calc_rho1 (fpr, fnr, q), num_of_pos_inds)
    return cost_ECM_FNA

def print_addplot (X, Y, NORMALIZE, plot_range):
    if (NORMALIZE):
        Y = Y / C_PI
    for i in plot_range:
        print ('(%.2f,%.3f)' % (X[i], Y[i]), end = '')
    print ('')

########################################################################################################
# main for plots where the X-axis is the hit rate
########################################################################################################
n= 3 # Num of caches
missp = 100;

NUM_OF_PHIT_VALS = 20
safe_range = range(1, NUM_OF_PHIT_VALS) # safe_range Will be used to prevent divisions by 0 in edge cases
Phit = np.array (safe_range).astype('float') / NUM_OF_PHIT_VALS
Pmiss = np.ones(NUM_OF_PHIT_VALS-1) - Phit; #miss rate
indicator_cost = 0 # Cost of using an indicator
NUM_OF_VALS = NUM_OF_PHIT_VALS-1

C_PI = cost_of_perfect_indicator (n)

fpr, fnr = 0.01, 0.01
NORMALIZE = True

# print ('n = ', n, 'missp = ', missp, ', $\\fpr = %.2f, \\fnr =%.2f$'    %(fpr, fnr))
plot_range = range (0, NUM_OF_PHIT_VALS-1)
# print_addplot (Phit, cost_of_imperfect_indicator_FNO (n, fpr, fnr), NORMALIZE, plot_range)
# print_addplot (Phit, cost_of_imperfect_indicator_FNA (n, fpr, fnr), NORMALIZE, plot_range)

# Calculate the cost of No-Indicator
fpr, fnr = 1, 0 # Dummy False Positive Rate, False Negative Rate for calculating the NI best practice
num_of_pos_inds = n
C_NI = np.ones (NUM_OF_PHIT_VALS-1)
for i in range (NUM_OF_PHIT_VALS-1):
    rho0, rho1 = Pmiss[i], Pmiss[i] # in the lack of an indicator, both rho0, rho1 are the miss ratio
    num_of_pos_inds = n
    k1 = calc_k_opt_NI (rho1, num_of_pos_inds, missp)
    k0 = 0
    C_NI[i] = cost (k0, k1, rho0, rho1, missp)
    print ('(%.2f,%.3f)' % (Phit[i], C_NI[i]/C_PI[i]), end = '')
# print (C_NI)

# # ########################################################################################################
# # Heatmap where the X, Y axes are fpr, fnr
# # ########################################################################################################
# n = 5 # Num of caches
# missp = 100;
# Phit = 0.5
# Pmiss = 1 - Phit
# DELTA_FPR = 0.005 
# NUM_OF_VALS = 10
# MAX_FPR = DELTA_FPR * NUM_OF_VALS
# fpr = np.array = np.arange (0, MAX_FPR, DELTA_FPR)
# fnr = np.array = np.arange (0, MAX_FPR, DELTA_FPR)
# C_PI = cost_of_perfect_indicator (n)

# NORMALIZE = True

# print ('n = ', n, ', Phit = ', Phit, ', FNA')
# for fnr_idx in range (NUM_OF_VALS):
    # cost_FNA = cost_of_imperfect_indicator_FNA (n, fpr, fnr[fnr_idx])
    # if (NORMALIZE):
        # cost_FNA = cost_FNA / C_PI
    # for fpr_idx in range (NUM_OF_VALS):
        # print('%.4f %.4f %.4f' % (fpr[fpr_idx], fnr[fnr_idx], cost_FNA[fpr_idx]))
    # print ('')

# print ('n = ', n, ', Phit = ', Phit, ', FNO')
# for fnr_idx in range (NUM_OF_VALS):
    # cost_FNO = cost_of_imperfect_indicator_FNO (n, fpr, fnr[fnr_idx])
    # if (NORMALIZE):
        # cost_FNO = cost_FNO / C_PI
    # for fpr_idx in range (NUM_OF_VALS):
        # print('%.4f %.4f %.4f' % (fpr[fpr_idx], fnr[fnr_idx], cost_FNO[fpr_idx]))
    # print ('')


# # ##########################################################################################################
# # main for a plots where X is n.
# # when SCALE_P_HIT=False, Phit is const.
# # Else, Phit is or 1/(n+1).
# # ###########################################################################################################
# missp = 100;
# Phit = 0.4
# Pmiss = 1 - Phit
# max_n = 10
# NUM_OF_VALS=1
# fpr, fnr = 0.03, 0.03
#
# # Calc the cost of NI
# # C_NI[n] will hold the service cost of NI when there're n caches.
# C_NI = np.zeros (max_n+1)
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit = 1/(n+1)
#         Pmiss = 1 - Phit
#     C_NI [n] = calc_no_indicator (n)

# SCALE_P_HIT = True
# # Calculate and print PI
# print ('\\addplot[color=black, mark=triangle*, solid] coordinates {')
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1/(n+1), n/(n+1)
#     print ('(%d, %.2f)' %(n, cost_of_perfect_indicator(n)/C_NI[n]), end = '')
# print ('};	\\addlegendentry{PI}')
#
# print ('\\addplot[color=black, mark=+, dashed] coordinates {')
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1/(n+1), n/(n+1)
#     print ('(%d, %.2f)' %(n, calc_imperfect_indicator(n, fpr, fnr)/C_NI[n]), end = '')
# print ('};	\\addlegendentry{II}')
#
# print ('\\addplot[color=black, mark=o, dotted, ultra thick] coordinates {')
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1/(n+1), n/(n+1)
#     print ('(%d, %.2f)' %(n, calc_imperfect_indicator_FNO(n, fpr, fnr)/C_NI[n]), end = '')
# print ('};	\\addlegendentry{II, FNO}')

# # ########################################################################################################
# # main for heatmap where the X, Y axes are n, fnr
# # ########################################################################################################
# missp = 100;
# Phit = 0.3
# Pmiss = 1 - Phit
# max_n = 5
# NUM_OF_VALS=1
# SCALE_P_HIT = True # when SCALE_P_HIT=False, Phit is const. Else, Phit is or 1/(n+1).
# NORMALIZE = True
#
#
# # Calc the cost of NI
# # C_NI[n] will hold the service cost of NI when there're n caches.
# C_NI = np.zeros (max_n+1)
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1/(n+1), n/(n+1)
#     C_NI [n] = calc_no_indicator (n)
#
# fpr = 0.02
# DELTA_FNR = 0.005
# NUM_OF_VALS = 10
# MAX_FNR = DELTA_FNR * NUM_OF_VALS
# fnr = np.array = np.arange (0, MAX_FNR, DELTA_FNR)
#
# print ('Phit = ', Phit, ', \\ecm')
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1 / (n + 1), n / (n + 1)
#     for fnr_idx in range (NUM_OF_VALS):
#         C_II = calc_imperfect_indicator (n, fpr, fnr[fnr_idx])
#         if (NORMALIZE):
#             C_II = C_II / C_NI[n]
#         print('%d %.4f %.4f' % (n, fnr[fnr_idx], C_II[fnr_idx]))
#     print ('')
#
# print ('Phit = ', Phit, ', \\ecm')
# for n in range (1, max_n+1):
#     if (SCALE_P_HIT):
#         Phit, Pmiss = 1 / (n + 1), n / (n + 1)
#     for fnr_idx in range (NUM_OF_VALS):
#         C_II = calc_imperfect_indicator_FNO (n, fpr, fnr[fnr_idx])
#         if (NORMALIZE):
#             C_II = C_II / C_NI[n]
#         print('%d %.4f %.4f' % (n, fnr[fnr_idx], C_II[fnr_idx]))
#     print ('')

# # ########################################################################################################
# # main for heatmap where the X, Y axes are n, Phit
# # ########################################################################################################
# missp = 100;
# max_n = 8
# NUM_OF_VALS=10
# Phit_vec = np.array (range(1, NUM_OF_VALS) ).astype('float') / NUM_OF_VALS
#
# for n in range (1, max_n+1):
#     for Phit_idx in range (NUM_OF_VALS-1):
#         Phit = Phit_vec[Phit_idx]
#         Pmiss = 1 - Phit
#         print('%d %.4f %.4f' % (n, Phit, cost_of_perfect_indicator(n) / calc_no_indicator(n)[0]))
#


#############################################################################################
# Accessory function for plotting a single plot in a  format fitting to tikz
# Inputs:
# X, Y axes.
# NORMALIZE - when true, normal the y plot w.r.t. NI (No Indicator)
# plot_range - the range of the X, Y to plot.

