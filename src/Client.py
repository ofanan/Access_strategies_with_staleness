import numpy as np
import math

from MyConfig import exponential_window

class Request(object):
    """
    """
    
    def __init__(self, ID, client_id, key):
        self.ID         = ID # Request id
        self.client_id  = client_id # Client to which this request is assigned
        self.key        = key # key of the datum the client is looking for
        
class Client(object):
    
    def __init__(self, ID, num_of_DSs, estimation_window  = 1000, window_alpha = 0.25, verbose = 0, 
                use_redundan_coef = False, k_loc = 1, use_adaptive_alg = False, missp = 100):
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
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs , dtype='uint16') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.q_estimation       = 0.5 * np.ones (self.num_of_DSs) # q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.first_estimate     = True # indicates whether this is the first estimation window
        self.estimation_window  = np.uint16 (estimation_window) # Number of requests performed by this client during each window
        self.window_alpha       = window_alpha # window's alpha parameter 
        self.fpr                = np.zeros (self.num_of_DSs) # fpr[i] will hold the estimated False Positive Rate of DS i
        self.fnr                = np.zeros (self.num_of_DSs) # fnr[i] will hold the estimated False Negative Rate of DS i
        self.zeros_ar           = np.zeros (self.num_of_DSs) 
        self.ones_ar            = np.ones  (self.num_of_DSs) 
        self.redundan_coef      = k_loc / self.num_of_DSs # Redundancy coefficient, representing the level of redundancy of stored items
        self.use_redundan_coef  = False  
        self.speculate_hit_cnt  = 0
        self.speculate_accs_cost = 0
        self.use_adaptive_alg   = use_adaptive_alg
        self.missp              = missp
        self.throttle           = False

        if (use_redundan_coef and self.redundan_coef > math.exp(1)):
            self.use_redundan_coef  = True # A boolean variable, determining whether to consider the redundan' coef' while calculating mr_0
            self.redundan_coef      = math.log (self.redundan_coef)
        # Debug
        # dictionary describing for every req_id of client: 0: init, 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer beta, 4: no pos ind, pay beta
        # self.action 			= {}
        self.verbose            = verbose
        if (self.verbose == 1):
            self.DS_accessed 		= {} # dictionary containing DSs accessed for every req_id of client where access takes place. Currently used only in higher-verbose modes.
            self.num_DS_accessed 	= [] # Total Num of DSs accessed by this client perrequest. Currently unused

    def add_DS_accessed(self, req_id, DS_index_list):
        """
        Logs the identity and the number of DSs accessed upon each request. Called only when verbose > 0 
        """
        self.DS_accessed[req_id] = DS_index_list
        self.num_DS_accessed.append(len(DS_index_list))
        

#    def get_mr (self, pos_DS_list):
    def get_mr (self, indications):
        """
        Calculate and return the expected miss prob' of each DS, based on its indication.
        Input: indications - a vector, where indications[i] is true  iff indicator i gave a positive indication.
        Details: The func' does the following:  
        - Increment ind_cnt (the cntr of queries to each indicator). Currently we always query all indicators, so 1 cntr suffices for all indicators together  
        - Increment pos_ind_cnt[j] (the cntr of queries to indicator i in the current window), for each indicator j which gave positive indication  
        - Update the estimation of q. q[i] holds the prob' that indicator i gives positive indication
        - Update mr0 (the expected prob' of a miss, given a negative indication), and mr1 (the expected prob' of a miss, given a positive indication).
        - Returns the vector mr, where mr[i] is the estimated miss ratio of DS i, given its indication
        """
        self.ind_cnt += 1 # Received a new set of indications
        self.pos_ind_cnt += indications #self.pos_ind_cnt[i]++ iff (indications[i]==True)
        if (self.ind_cnt < self.estimation_window): # Init period - use merely the data collected so far
            self.q_estimation   = self.pos_ind_cnt/self.estimation_window
        elif (self.ind_cnt % self.estimation_window): # run period - update the estimation once in a self.estimation_window time
            self.q_estimation   = exponential_window (self.q_estimation, self.pos_ind_cnt/self.estimation_window, self.window_alpha)
            self.pos_ind_cnt = np.zeros (self.num_of_DSs , dtype='uint16') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window

        hit_ratio = np.maximum (self.zeros_ar, (self.q_estimation - self.fpr) / (1 - self.fpr - self.fnr))
        if (self.use_adaptive_alg):
            if (self.speculate_accs_cost > 10 * self.missp and self.speculate_hit_cnt < 10): #Collected enough history, and realized that we only loose from speculations
                self.throttle             = True
                self.speculative_efficiency_factor = self.speculate_hit_cnt * self.missp / self.speculate_accs_cost  
                self.speculate_accs_cost  = 0
                self.speculate_hit_cnt    = 0
        for i in range (self.num_of_DSs):
            if (indications[i]):
                if (self.fpr[i] == 0): # No false positives at this DS
                    self.mr[i] = 0     #
                else:
                    self.mr[i] = 1 if (self.q_estimation[i] == 0) else self.fpr[i] * (1 - hit_ratio[i]) / self.q_estimation[i] # if DS i gave pos' ind', then mr[i] will hold the estimated prob' that a datum is not in DS i, given that pos' indication for x        
            else:
                self.mr[i] = 1 if (self.fnr[i] == 0 or self.q_estimation[i] == 1) else (1 - self.fpr[i]) * (1 - hit_ratio[i]) / (1 - self.q_estimation[i]) # if DS i gave neg' ind', then the estimated prob' that a datum is not in DS i, given a neg' indication for x
                if (self.use_redundan_coef and self.mr[i] != 1):
                    self.mr[i] = 1 - (1 - self.mr[i]) / self.redundan_coef
                if (self.throttle):
                    self.mr[i] = 1 - (1 - self.mr[i]) * self.speculative_efficiency_factor

        self.mr = np.minimum (self.mr, self.ones_ar)
        if (self.verbose == 2):
            print ('id = ', self.ID, 'q_estimation = ', self.q_estimation, 'fpr = ', self.fpr, 'fnr = ', self.fnr, 'hit ratio = ', hit_ratio, 'mr = ', self.mr)
        return self.mr

    def update_fnr_fpr (self, fnr_fpr, DS_id):
        """
        Returns fnr_fpr - a vector, where fnr_fpr[0] is the expected fnr, and fnr_fpr[1] is the expected fpr
        DS_id - id of DS for which these are the estiamted fnr and fpr
        """
        self.fnr[DS_id] = fnr_fpr[0]
        self.fpr[DS_id] = fnr_fpr[1]
