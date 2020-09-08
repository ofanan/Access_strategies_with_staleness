import numpy as np

from MyConfig import exponential_window

class Request(object):
    
    def __init__(self, ID, client_id, key):
        self.ID = ID
        self.client_id = client_id
        self.key = key

        
class Client(object):
    
    def __init__(self, ID, num_of_DSs):
        """
        Return a Client object with the following attributes:
            ID:                 client ID 
        """
        self.ID                 = ID
        self.total_cost         = 0
        self.total_access_cost  = 0
        self.req_cnt            = 0
        self.access_cnt         = 0
        self.hit_cnt            = 0
        self.high_cost_mp_cnt   = 0
        self.comp_miss_cnt      = 0
        self.non_comp_miss_cnt  = 0
        self.num_of_DSs         = num_of_DSs # Number of DSs in the system

        self.cur_mr_list 		= {} # dictionary containing the mr list req_id of client
        self.DS_accessed 		= {} # dictionary containing DSs accessed for every req_id of client where access takes place. Currently unused
        self.num_DS_accessed 	= [] # Total Num of DSs accessed by this client perrequest. Currently unused
        self.ind_cnt            = 0  # Number of indications requested by this client during this window 
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs , dtype='uint') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.q_estimation       = np.zeros (self.num_of_DSs) #q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.estimation_window  = 1000 # Number of requests performed by this client during each window
        self.window_alhpa       = 0.1 # window's alpha parameter 
        self.mr0                = np.ones  (self.num_of_DSs) # mr0[i] will hold the estimated prob' that a datum is not in DS i, given a neg' indication for x
        self.mr1                = np.zeros (self.num_of_DSs) # mr1[i] will hold the estimated prob' that a datum is not in DS i in spite of a pos' indication for x

        # Debug
        # dictionary describing for every req_id of client: 0: init, 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer beta, 4: no pos ind, pay beta
        # self.action 			= {}

    def add_DS_accessed(self, req_id, DS_index_list):
        """
        Logs the identity and the number of DSs accessed upon each request. Used only when verbose > 0 
        """
        self.DS_accessed[req_id] = DS_index_list
        self.num_DS_accessed.append(len(DS_index_list))
        

    def get_mr (self, pos_DS_list):
        """
        Increment ind_cnt (the cntr of queries to each indicator). Currently we always query all indicators, so 1 cntr suffices for all indicators together  
        Increment pos_ind_cnt[j] (the cntr of queries to indicator i in the current window), for each indicator j which gave positive indication  
        Update the estimation of q. q[i] holds the prob' that indicator i gives positive indication
        Returns mr, where mr[i] is the estimated miss ratio of DS i, given its indication
        """
        self.ind_cnt += 1
        for i in pos_DS_list:
            self.pos_ind_cnt[i] += 1    
        if (self.ind_cnt % self.estimation_window == 0):
            self.q_estimation = exponential_window (self.q_estimation, self.pos_ind_cnt/self.estimation_window, self.window_alhpa)

        mr = np.zeros (self.num_of_DSs)
        for i in range (self.num_of_DSs):
            if i in pos_DS_list:
                mr[i] = self.mr1[i]
            else:
                mr[i] = self.mr0[i]
        return mr

    def update_mr0_mr1 (self, fp, fn, DS_id):
        """
        fp - the estimated false positive rate
        fn - the estimated false negative rate
        DS_id - id of DS for which these are the estiamted fp and fn
        """
