import numpy as np
import mod_pylru
import itertools
import CountingBloomFilter as CBF
import copy
import MyConfig 


class DataStore (object):
    
    def __init__(self, ID, size = 1000, bpe = 5, mr1_window_alpha = 0.25, mr1_estimation_window = 100, 
                 max_fnr = 0.03, max_fpr = 0.03, verbose = 0, uInterval = 1,
                 num_of_insertions_between_estimations = 20):
        """
        Return a DataStore object with the following attributes:
            ID:                 datastore ID 
            size:               number of elements that can be stored in the datastore
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            mr1_window_alpha:    sliding window parameter for miss-rate estimation 
            mr1_estimation_window:  Number of regular accesses between new performing new estimation of mr1 (prob' of a miss given a pos' ind'). 
            max_fnr, max_fpr : maximum allowed (estimated) fpr, fnr. When the estimated fnr is above max_fnr, or the estimated fpr is above mx_fpr, the DS sends an update.
                               (FPR: False Positive Ratio, FNR: False Negative Ratio).
                               currently usually unused, as the time to update is defined exclusively by the update interval.
            num_of_insertions_between_estimations : number of cache insertions between performing fresh estimations of fpr, and fnr.
                - Each time a new indicator is published, the update contains a fresh estimation, and a counter is reset. Then, each 
                  time the counter reaches num_of_insertions_between_estimations. a new fpr and fnr estimation is published, and the counter is reset.  
            verbose:           how much details are written to the output
        """
        self.ID                     = ID
        self.cache_size             = size
        self.bpe                    = bpe
        self.BF_size                = self.bpe * self.cache_size
        self.lg_BF_size             = np.log2 (self.BF_size) 
        self.num_of_hashes          = MyConfig.get_optimal_num_of_hashes (self.bpe)
        self.designed_fpr           = MyConfig.calc_designed_fpr (self.cache_size, self.BF_size, self.num_of_hashes)
        self.mr1_window_alpha       = mr1_window_alpha
        self.mr0_window_alpha       = mr1_window_alpha
        self.mr1_estimation_window  = mr1_estimation_window
        self.mr0_estimation_window  = mr1_estimation_window
        self.one_min_mr1_alpha      = 1 - self.mr1_window_alpha
        self.one_min_mr0_alpha      = 1 - self.mr0_window_alpha
        self.mr1_alpha_over_window  = float (self.mr1_window_alpha) / float (self.mr1_estimation_window)
        self.mr0_alpha_over_window  = float (self.mr0_window_alpha) / float (self.mr0_estimation_window)
        self.fp_events_cnt          = int(0) # Number of False Positive events that happened in the current estimatio window
        self.tn_events_cnt          = int(0) # Number of False Positive events that happened in the current estimatio window
        self.reg_accs_cnt           = 0
        self.spec_accs_cnt          = 0
        self.max_fnr                = max_fnr
        self.max_fpr                = max_fpr
        self.updated_indicator      = CBF.CountingBloomFilter (size = self.BF_size, num_of_hashes = self.num_of_hashes)
        self.stale_indicator        = self.updated_indicator.gen_SimpleBloomFilter ()         
        self.mr1_cur                = 0.5 
        self.mr0_cur                = 1 # Initially assume that there're no FN events, that is: the miss prob' in case of a negative ind' is 1.
        self.cache                  = mod_pylru.lrucache(self.cache_size) # LRU cache. for documentation, see: https://pypi.org/project/pylru/
        self.fnr                    = 0 # Initially, there are no false indications
        self.fpr                    = 0 # Initially, there are no false indications
        self.delta_th               = self.BF_size / self.lg_BF_size # threshold for number of flipped bits in the BF; below this th, it's cheaper to send only the "delta" (indices of flipped bits), rather than the full ind'         
        self.update_bw              = 0
        self.num_of_updates         = 0
        self.verbose                = verbose #if self.ID==0 else 0
        self.ins_cnt                = np.uint32 (0)
        self.num_of_fpr_fnr_updates = int (0) 
        self.use_only_updated_ind   = True if (uInterval == 1) else False
        self.uInterval              = uInterval if (self.use_only_updated_ind == False) else float('inf')
        
        self.num_of_insertions_between_estimations  = num_of_insertions_between_estimations
        self.ins_since_last_fpr_fnr_estimation      = int (0)
        if (self.verbose == 3):
            self.debug_file = open ("../res/fna.txt", "w")

    def __contains__(self, key):
        """
        test to see if key is in the cache
        enables using the syntax:
            key in datastore
        """
        return (key in self.cache)
            
    def access(self, key, is_speculative_accs = False):
        """
        - Accesses a key in the cache.
        - Return True iff the access was a hit.
        - Update the relevant cntrs (regular / spec access cnt, fp / tn cnt).
        - Update the mr0, mr1 (prob' of a miss, given a neg / pos ind'), if needed.
        """
        hit = True if (key in self.cache) else False          
        if hit: 
            self.cache[key] #Touch the element, so as to update the LRU mechanism

        if (is_speculative_accs):
            self.spec_accs_cnt += 1
            if (not(hit)):
                self.tn_events_cnt += 1
            if (self.spec_accs_cnt % self.mr0_estimation_window == 0):
                self.update_mr0()
        else: # regular accs
            self.reg_accs_cnt  += 1
            if (not(hit)):
                self.fp_events_cnt += 1
            if (self.reg_accs_cnt % self.mr1_estimation_window == 0):
                self.update_mr1()
        return hit 

    def insert(self, key, use_indicator = True, req_cnt = -1, consider_fpr_fnr_update = True):
        """
        - Inserts a key to the cache
        - Update the indicator
        - Check if it's time to send an update
        if key is already in the cache: return False
        otherwise: return True
        """
            # check to see if the insertion will cause the LRU key to be removed
            # if so, remove it from the updated indicator
			# Removal from the cache is implemented automatically by the cache object
        self.cache[key] = key
        if (use_indicator):
            if (self.cache.currSize() == self.cache.size()):
                self.updated_indicator.remove(self.cache.get_tail())
            self.updated_indicator.add(key)
            self.ins_cnt                += 1
            self.ins_since_last_fpr_fnr_estimation  += 1
            if (consider_fpr_fnr_update):
                if (self.ins_since_last_fpr_fnr_estimation == self.num_of_insertions_between_estimations): 
                    self.estimate_fnr_fpr (req_cnt) # Update the estimates of fpr and fnr, and check if it's time to send an update
                    self.num_of_fpr_fnr_updates += 1
                    self.ins_since_last_fpr_fnr_estimation = 0
            if (self.should_send_update() ):
                self.send_update ()
                
    def get_indication (self, key):
        """
        Query the (stale) indicator of this DS
        """
        if (self.use_only_updated_ind):
            return (key in self.updated_indicator)
        return (key in self.stale_indicator)

    def send_update (self, check_delta_th = False):
        """
        Send an updated indicator.
        If the input check_delta_th, then calculate the "deltas", namely, number of bits set / reset since the last update has been advertised.
        """
            
        self.num_of_updates += 1
        if (check_delta_th):
            updated_sbf = self.updated_indicator.gen_SimpleBloomFilter ()
            Delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)), sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array))]
            if (sum (Delta) < self.delta_th):
                print ('sum_Delta = ', sum (Delta), 'delta_th = ', self.delta_th, 'Sending delta updates is cheaper\n')
                exit ()
        self.stale_indicator                    = self.updated_indicator.gen_SimpleBloomFilter () # "stale_indicator" is the snapshot of the current state of the ind', until the next update
        B1_st                                   = sum (self.stale_indicator.array)    # Num of bits set in the updated indicator
        self.fpr                                = pow ( B1_st / self.BF_size, self.num_of_hashes)
        self.fnr                                = 0 # Immediately after sending an update, the expected fnr is 0
        self.ins_since_last_fpr_fnr_estimation  = 0

    def update_mr0(self):
        """
        update the miss-probability in case of a positive indication, using an exponential moving average.
        """
        self.mr0_cur = self.mr0_alpha_over_window * float(self.tn_events_cnt) + self.one_min_mr0_alpha * self.mr0_cur 
        self.fn_events_cnt = int(0)
        
    def update_mr1(self):
        """
        update the miss-probability in case of a positive indication, using an exponential moving average.
        """
        self.mr1_cur = self.mr1_alpha_over_window * float(self.fp_events_cnt) + self.one_min_mr1_alpha * self.mr1_cur 
        self.fp_events_cnt = int(0)
        
    def get_mr(self): 
        """
        get the current miss-rate estimate
        """
        return self.mr1_cur

    def print_cache(self, head = 5):
        """
        test to see if key is in the cache
        """
        for i in itertools.islice(self.cache.dli(),head):
            print (i.key)
    
    def should_send_update (self):
        """
        Returns true iff an updated indicator should be sent.
        """
#         if (self.fnr > self.max_fnr or self.fpr > self.max_fpr):
#             return True 
        if (self.ins_cnt % self.uInterval == 0):
            return True
        return False
            

    def estimate_fnr_fpr (self, req_cnt = -1, key = -1):
        """
        Estimates the fnr and fpr, based on Theorems 3 and 4 in the paper: "False Rate Analysis of Bloom Filter Replicas in Distributed Systems".
        The new values are written to self.fnr_fpr, where self.fnr_fpr[0] is the fnr, and self.fnr_fpr[1] is the fpr
        The optional inputs req_cnt and key are used only for debug 

        """
        updated_sbf = self.updated_indicator.gen_SimpleBloomFilter ()
        # Delta[0] will hold the # of bits that are reset in the updated array, and set in the stale array.
        # Delta[1] will hold the # of bits that are set in the updated array, and reset in the stale array.
        Delta       = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)), sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array))]
        B1_up       = sum (updated_sbf.array)             # Num of bits set in the updated indicator
        B1_st       = sum (self.stale_indicator.array)    # Num of bits set in the stale indicator
        self.fnr    = 1 - pow ( (B1_up-Delta[1]) / B1_up, self.num_of_hashes)
        self.fpr    = pow ( B1_st / self.BF_size, self.num_of_hashes)

#         if (self.should_send_update()==True): # either the fpr or the fnr is too high - need to send update
#             size_of_delta_update = sum (Delta)     
#             self.update_bw += (size_of_delta_update * self.lg_BF_size) if (size_of_delta_update < self.delta_th) else self.BF_size     
#             self.stale_indicator.array = updated_sbf.array # Send update
#             self.num_of_updates += 1
#             self.fnr = 0
#             self.fpr = pow ( B1_up / self.BF_size, self.num_of_hashes) # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr

