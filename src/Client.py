"""
The client-side algorithm (CS_{FNA}), described in the paper:
"On the Power of False Negative Awareness in Indicator-based Caching Systems", Cohen, Einziger, Scalosub, ICDCS'21.
"""

import numpy as np
import math

from printf import printf

class Request(object):
    """
    """
    
    def __init__(self, ID, client_id, key):
        self.ID         = ID # Request id
        self.client_id  = client_id # Client to which this request is assigned
        self.key        = key # key of the datum the client is looking for
        
class Client(object):
    
    def __init__(self, ID, num_of_DSs, estimation_window  = 1000, window_alpha = 0.25, verbose = 0, 
                use_redundan_coef = False, k_loc = 1, use_adaptive_alg = False, missp = 100, verbose_file = None):
        """
        Return a Client object with the following attributes:
        """
        self.ID                 = ID
        self.num_of_DSs         = num_of_DSs # Number of DSs in the system
        self.non_comp_miss_cnt  = 0
        self.comp_miss_cnt      = 0
        self.high_cost_mp_cnt   = 0
        self.hit_cnt            = 0
        self.access_cnt         = 0
        self.total_access_cost  = 0

        self.mr                 = np.zeros (self.num_of_DSs) # mr[i] will hold the estimated prob' of a miss, given the ind' of DS[i]'s indicator
        self.ind_cnt            = 0  # Number of indications requested by this client during this window 
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs) #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.q_estimation       = 0.5 * np.ones (self.num_of_DSs) # q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.first_estimate     = True # indicates whether this is the first estimation window
        self.estimation_window  = np.uint16 (estimation_window) # Number of requests performed by this client during each window
        self.window_alpha       = window_alpha # window's alpha parameter 
        self.one_min_alpha      = 1 - self.window_alpha
        self.alpha_over_window  = float (self.window_alpha) / float (self.estimation_window)
        self.fpr                = np.zeros (self.num_of_DSs) # fpr[i] will hold the estimated False Positive Rate of DS i
        self.fnr                = np.zeros (self.num_of_DSs) # fnr[i] will hold the estimated False Negative Rate of DS i
        self.zeros_ar           = np.zeros (self.num_of_DSs) 
        self.ones_ar            = np.ones  (self.num_of_DSs) 
        self.redundan_coef      = k_loc / self.num_of_DSs # Redundancy coefficient, representing the level of redundancy of stored items
        self.speculate_hit_cnt  = 0
        self.speculate_accs_cost = 0
        self.use_adaptive_alg   = use_adaptive_alg
        self.missp              = missp
        self.use_spec_factor    = False

        self.use_redundan_coef  = False  
#         if (use_redundan_coef and self.redundan_coef > math.exp(1)):
#             self.use_redundan_coef  = True # A boolean variable, determining whether to consider the redundan' coef' while calculating mr_0
#             self.redundan_coef      = math.log (self.redundan_coef)
        # Debug
        # dictionary describing for every req_id of client: 0: init, 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer beta, 4: no pos ind, pay beta
        # self.action 			= {}
        self.verbose            = verbose
        self.verbose_file       = verbose_file
        
        if (self.verbose == 1):
            self.DS_accessed 		= {} # dictionary containing DSs accessed for every req_id of client where access takes place. Currently used only in higher-verbose modes.
            self.num_DS_accessed 	= [] # Total Num of DSs accessed by this client perrequest. Currently unused
            
    def add_DS_accessed(self, req_id, DS_index_list):
        """
        Logs the identity and the number of DSs accessed upon each request. Called only when verbose > 0 
        """
        self.DS_accessed[req_id] = DS_index_list
        self.num_DS_accessed.append(len(DS_index_list))
        
    def estimate_Pone_and_hit_ratio (self, indications):
        """
        Estimate Pone (aka "q") - the probability of positive indication; and the hit ratio of each DS.
        Details: The func' does the following:  
        - Increment ind_cnt (the cntr of queries to each indicator). Currently we always query all indicators, so 1 cntr suffices for all indicators together  
        - Increment pos_ind_cnt[j] (the cntr of queries to indicator i in the current window), for each indicator j which gave positive indication  
        - Update the estimation of q. q[i] holds the prob' that indicator i gives positive indication
        """
        self.ind_cnt += 1 # Received a new set of indications
        self.pos_ind_cnt += indications 
        if (self.ind_cnt < self.estimation_window ): # Init period - use merely the data collected so far
            self.q_estimation   = self.pos_ind_cnt/self.estimation_window
        elif (self.ind_cnt % self.estimation_window == 0): # run period - update the estimation once in a self.estimation_window time
            if (self.verbose == 3 and self.ID == 0):
                print ('q = ', self.q_estimation, ', new q = ', self.pos_ind_cnt/self.estimation_window)
            self.q_estimation   = self.alpha_over_window * self.pos_ind_cnt + self.one_min_alpha * self.q_estimation
            self.pos_ind_cnt    = np.zeros (self.num_of_DSs , dtype='uint16') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window

        self.hit_ratio = np.minimum (self.ones_ar, np.maximum (self.zeros_ar, (self.q_estimation - self.fpr) / (1 - self.fpr - self.fnr)))

    def estimate_mr1_mr0_by_analysis (self, indications, # vector, where indications[i] is true  iff indicator i gave a positive indication. 
                                      fno_mode  = False, # When True, discard false-negatives, by assuming that mr0 is always 1 (namely, a negative ind' is always true)
                                      update_mr = True,  # When True, not only estimate the mr, but also write it to self.mr, so that this would be the next estimation 
                                      verbose=0, ):
        """
        Calculate and return the expected miss prob' ("exclusion probability") of each DS
        Details: The func' does the following:  
        - Update the estimations of Pone ("q") and the hit ratio.
        - For each DS:
        - - If indication[i] == True, then assign mr[i] = mr1[i], according to the given history vector. 
        - - Else:
        - - - If we're in fno-mode (False-Negative-Oblivious mode), then mr[i]=0, as we assume that a negative ind' is always true (the item is surely not in the cache).
        - - - Else, assign mr[i] = mr0[i], as estimated by our analysis.
        - Returns the vector mr, where mr[i] is the estimated miss ratio of DS i, given its indication
        """
        mr = np.zeros (self.num_of_DSs)
        self.estimate_Pone_and_hit_ratio (indications)
        for i in range (self.num_of_DSs):
            if (indications[i]): #positive ind'
                
                if (self.q_estimation[i] == 0): 
                    mr[i] = 1
                elif (self.fpr[i] == 0 or # If there're no FP, then upon a positive ind', the prob' that the item is NOT in the cache is 0 
                      self.hit_ratio[i] == 1): #If the hit ratio is 1, then upon ANY indication (and, in particular, positive ind'), the prob' that the item is NOT in the cache is 0
                        mr[i] = 0 
                else:
                    mr[i] = self.fpr[i] * (1 - self.hit_ratio[i]) / self.q_estimation[i]
            else:                                                
                mr[i] = 1 if (fno_mode or 
                                   self.fnr[i] == 0 or # if there're no false-neg, then upon a negative ind', the item is SURELY not in the cache  
                                   self.q_estimation[i] == 1 or # if q_estimation is 1, the denominator of our formula is 0, so mr[i] should get its maximal value --> 1  
                                   self.hit_ratio[i] == 1) else (1 - self.fpr[i]) * (1 - self.hit_ratio[i]) / (1 - self.q_estimation[i]) 

        mr = np.maximum (self.zeros_ar, np.minimum (mr, self.ones_ar)) # Verify that all mr values are feasible - that is, within [0,1].
        if (update_mr):
            self.mr = mr
        return mr


    def get_mr_given_mr1 (self, indications, mr0, mr1, verbose):
        """
        Calculate and return the expected miss prob' of each DS, based on its indication.
        Input: 
        indications - a vector, where indications[i] is true iff indicator i gave a positive indication.
        mr0 - a vector. mr0[i] is the estimation (based on historic data) of the miss probab' in cache i, given a neg' ind' by indicator i
        mr1 - a vector. mr1[i] is the estimation (based on historic data) of the miss probab' in cache i, given a pos' ind' by indicator i        
        Details: The func' does the following:  
        - Update the estimations of Pone ("q") and the hit ratio.
        - For each DS:
        - - If indication[i] == True, then assign mr[i] = mr1[i], according to the given history vector. 
        - - Else, assign mr[i] = mr0[i], as estimated by our analysis.
        - - Handle corner cases (e.g., probabilities calculated are below 0 or above 1)
        - Returns the vector mr, where mr[i] is the estimated miss ratio of DS i, given its indication
        """
        self.estimate_Pone_and_hit_ratio (indications)
        
        for i in range (self.num_of_DSs):
            if (indications[i]): #positive ind'
                self.mr[i] = mr1[i]     #
            else:
                self.mr[i] = 1 if (self.fnr[i] == 0 or self.q_estimation[i] == 1 or self.hit_ratio[i]==1) \
                else (1 - self.fpr[i]) * (1 - self.hit_ratio[i]) / (1 - self.q_estimation[i]) # if DS i gave neg' ind', then the estimated prob' that a datum is not in DS i, given a neg' indication for x
                if (verbose == 4):
                    printf (self.verbose_file, 'mr_0[{}]: by analysis = {}, by hist = {}\n' .format (i, self.mr[i], mr0[i]))

        self.mr = np.maximum (self.zeros_ar, np.minimum (self.mr, self.ones_ar)) # Verify that all mr values are feasible - that is, within [0,1].
        return self.mr


