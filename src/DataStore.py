import numpy as np
import mod_pylru
import itertools
import CountingBloomFilter as CBF
import copy
import MyConfig 


class DataStore (object):
    
    def __init__(self, ID, size = 1000, bpe = 5, window_alpha = 0.25, estimation_window = 1000, max_fnr = 0.03, max_fpr = 0.03, verbose = 0, uInterval = 1):
        """
        Return a DataStore object with the following attributes:
            ID:                 datastore ID 
            size:               number of elements that can be stored in the datastore (default 1000)
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            window_alpha:    sliding window parameter for miss-rate estimation 
            estimation_window:  how often (queries) should the miss-rate estimation be updated (default 1000)
            max_fnr, max_fpr : maximum allowed (estimated) fpr, fnr. When the estimated fnr is above max_fnr, or the estimated fpr is above mx_fpr, the DS sends an update.
                               (FPR: False Positive Ratio, FNR: False Negative Ratio).
            verbose:           how much details are written to the output
        """
        self.ID                     = ID
        self.cache_size             = size
        self.bpe                    = bpe
        self.BF_size                = self.bpe * self.cache_size
        self.lg_BF_size             = np.log2 (self.BF_size) 
        self.num_of_hashes          = MyConfig.get_optimal_num_of_hashes (self.bpe)
        self.designed_fpr           = MyConfig.calc_designed_fpr (self.cache_size, self.BF_size, self.num_of_hashes)
        self.window_alpha           = window_alpha
        self.estimation_window      = estimation_window
        self.one_min_alpha          = 1 - self.window_alpha
        self.alpha_over_window      = float (self.window_alpha) / float (self.estimation_window)
        self.fp_events_cnt          = int(0)
        self.access_cnt             = 0
        self.hit_cnt                = 0
        self.max_fnr                = max_fnr
        self.max_fpr                = max_fpr
        self.updated_indicator      = CBF.CountingBloomFilter (size = self.BF_size, num_of_hashes = self.num_of_hashes)
        self.stale_indicator        = self.updated_indicator.gen_SimpleBloomFilter ()         
        self.mr_cur                 = 0.5 
        self.cache                  = mod_pylru.lrucache(self.cache_size) # LRU cache. for documentation, see: https://pypi.org/project/pylru/
        self.fnr                    = 0 # Initially, there are no false indications
        self.fpr                    = 0 # Initially, there are no false indications
        self.delta_th               = self.BF_size / self.lg_BF_size # threshold for number of flipped bits in the BF; below this th, it's cheaper to send only the "delta" (indices of flipped bits), rather than the full ind'         
        self.update_bw              = 0
        self.num_of_updates         = 0
        self.verbose                = verbose #if self.ID==0 else 0
        self.ins_cnt                = np.uint32 (0)
        self.num_of_fpr_fnr_updates = int (0)
        self.num_of_insertions_between_estimations = np.uint8 (max (self.cache_size / 1000, 1))
        self.uInterval = uInterval
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
        accesses a key in the cache, and inc the ins cntr.
        """
        self.access_cnt += 1
        
        # check to see if an update to the estimated miss-rate is required
        if (self.access_cnt % self.estimation_window == 0):
            self.update_mr1()
            # self.update_mr0()
        if key in self.cache: # hit
            self.cache[key] #Touch the element, so as to update the LRU mechanism
            self.hit_cnt += 1
            return True
        else: 
            if (not(is_speculative_accs)):
                self.fp_events_cnt += 1
            return False

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
            self.ins_cnt += 1
            if (consider_fpr_fnr_update):
                if ( self.ins_cnt % self.num_of_insertions_between_estimations == 0): 
                    self.estimate_fnr_fpr (req_cnt) # Update the estimates of fpr and fnr, and check if it's time to send an update
                    self.num_of_fpr_fnr_updates += 1
            if (self.should_send_update() ):
                self.send_update ()
                
    def get_indication (self, key):
        """
        Query the (stale) indicator of this DS
        """
        return (key in self.stale_indicator)

    def send_update (self, check_delta_th = False):
            
        self.num_of_updates += 1
        self.fnr = 0 # Immediately after sending an update, the expected fnr is 0
        updated_sbf = self.updated_indicator.gen_SimpleBloomFilter ()
        if (check_delta_th):
            Delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)), sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array))]
            if (sum (Delta) < self.delta_th):
                print ('sum_Delta = ', sum (Delta), 'delta_th = ', self.delta_th, 'Sending delta updates is cheaper\n')
                exit ()
        self.stale_indicator = self.updated_indicator.gen_SimpleBloomFilter ()
        B1_st = sum (self.stale_indicator.array)    # Num of bits set in the stale indicator
        self.fpr = pow ( B1_st / self.BF_size, self.num_of_hashes)
#         print ('d.fpr = {:.4f}, B1_st = {:.0f}, calc.fpr = {:.4f}' .format (self.designed_fpr, B1_st, pow ( B1_st / self.BF_size, self.num_of_hashes)))
#         self.fpr = self.designed_fpr # Immediately after sending an update, the expected fpr is the inherent fpr
        

        # self.updated_indicator.reset_delta_cntrs ()

    def update_mr1(self):
        """
        update the miss-rate estimate
        done using an exponential moving average.
        Used by False-Negative-Oblivious strategeis, such as the algorithms in the paper "Access Strategies for Network Caching."
        """
#         self.mr_estimate = (float(self.fp_events_cnt) / self.estimation_window)
        self.mr_cur = self.alpha_over_window * float(self.fp_events_cnt) + self.one_min_alpha * self.mr_cur 
        self.fp_events_cnt = int(0)
        
    def get_hr(self):
        """
        get the current overall hit-rate
        """
        if (self.access_cnt == 0):
            return 0
        return self.hit_cnt / self.access_cnt

    def get_mr(self): 
        """
        get the current miss-rate estimate
        """
        return self.mr_cur

    def print_cache(self, head = 5):
        """
        test to see if key is in the cache
        """
        for i in itertools.islice(self.cache.dli(),head):
            print (i.key)
    
    def should_send_update (self):
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
        Delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)), sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array))]
        B1_up = sum (updated_sbf.array)             # Num of bits set in the updated indicator
        B1_st = sum (self.stale_indicator.array)    # Num of bits set in the stale indicator
        self.fnr = 1 - pow ( (B1_up-Delta[1]) / B1_up, self.num_of_hashes)
        self.fpr = pow ( B1_st / self.BF_size, self.num_of_hashes)
#         if (self.should_send_update()==True): # either the fpr or the fnr is too high - need to send update
#             size_of_delta_update = sum (Delta)     
#             self.update_bw += (size_of_delta_update * self.lg_BF_size) if (size_of_delta_update < self.delta_th) else self.BF_size     
#             self.stale_indicator.array = updated_sbf.array # Send update
#             self.num_of_updates += 1
#             self.fnr = 0
#             self.fpr = pow ( B1_up / self.BF_size, self.num_of_hashes) # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr

