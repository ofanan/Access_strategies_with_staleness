import numpy as np 
from scipy.special import comb
import sys
from MyConfig import get_optimal_hash_count, exponential_window

bpe    = 16
k      = get_optimal_hash_count (bpe)
P1n    = 1 - np.exp (-k / bpe)
delta0 = 0.4
delta1 = delta0

calc_by_paper = pow (P1n, k) - pow (P1n - delta1, k)
# tmp = (1 - delta1 - delta0) #* P1n
# calc_by_me = pow (delta1 + tmp, k) - pow (tmp, k)
# by_me_directly = 0
# for i in range (1, k+1):
# 	by_me_directly += comb(k, i) * pow (delta1, i) * pow (tmp, k-i)
calc_by_me = 1 - pow ( (P1n - delta1) / P1n, k)
print ('P1n = ', P1n, ' by paper: ', calc_by_paper, 'by me: ', calc_by_me)	


# calc_directly = 0
# for i in range (1, k+1):
# 	calc_directly += comb(k, i) * pow (delta1, i) * pow (P1n - delta0, k-i)
# print ('calc directly: ', calc_directly)	
