import numpy as np
import mod_pylru
import itertools
import CountingBloomFilter as CBF
import copy
from MyConfig import get_optimal_hash_count, exponential_window


class DataStore (object):
    
    def __init__(self, ID, size = 1000, bpe = 5, window_alpha = 0.25, estimation_window = 1000, max_fnr = 0.04, max_fpr = 0.04, verbose = 0):
        """
        Return a DataStore object with the following attributes:
            ID:                 datastore ID 
            size:               number of elements that can be stored in the datastore (default 1000)
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            estimation_window:  how often (queries) should the miss-rate estimation be updated (default 1000, same as size)
            miss_rate_alpha:    sliding window parameter for miss-rate estimation (default 0.1)
        """
        self.ID                     = ID
        self.size                   = size
        self.bpe                    = bpe
        self.BF_size                = self.bpe * self.size 
        self.hash_count             = get_optimal_hash_count (self.bpe)
        self.window_alpha           = window_alpha
        self.estimation_window      = estimation_window
        self.mr_estimate            = []
        self.fp_events_cnt          = [0]
        self.FN_mr_estimate         = []
        self.fn_events_cnt          = [0]
        self.access_cnt             = 0
        self.hit_cnt                = 0
        self.max_fnr                = max_fnr
        self.max_fpr                = max_fpr
        self.P1n                    = 1 - np.exp (-self.hash_count / self.bpe)
        self.P1nk                   = pow (self.P1n, self.hash_count)
        self.updated_indicator      = CBF.CountingBloomFilter (size = self.BF_size, hash_count = self.hash_count)
        self.stale_indicator        = self.updated_indicator.gen_SimpleBloomFilter ()         
        self.mr_cur                 = [0.5] #[self.stale_indicator.get_designed_fpr ()]
        self.FN_mr_cur              = [1]
        self.cache                  = mod_pylru.lrucache(self.size) # LRU cache. for documentation, see: https://pypi.org/project/pylru/
        self.fnr                    = 0 # Initially, there are no false indications
        self.fpr                    = 0 # Initially, there are no false indications
        self.verbose                = verbose

    def __contains__(self, key):
        """
        test to see if key is in the cache
        enables using the syntax:
            key in datastore
        """
        return (key in self.cache)
            
    def access(self, key, is_speculative_accs = False):
        """
        accesses a key in the cache
        if access is successful (i.e., key is in cache): return True
        otherwise: return False
        """
        self.access_cnt += 1
        
        # check to see if an update to the estimated miss-rate is required
        if (self.access_cnt % self.estimation_window == 0):
            self.update_mr()
            # self.update_FN_mr()
        if key in self.cache: # hit
            self.cache[key] #Touch the element, so as to update the LRU mechanism
            self.hit_cnt += 1
            return True
        else:
            if (is_speculative_accs):
                self.fn_events_cnt[-1] += 1
            else:
                self.fp_events_cnt[-1] += 1
            return False

    def insert(self, key, use_indicator = True):
        """
        - Inserts a key to the cache
        - Update the indicator
        - Check if it's time to send an update
        if key is already in the cache: return False
        otherwise: return True
        """
        if (key in self.cache):
            return False
        else:
            # check to see if the insertion will cause the LRU key to be removed
            # if so, remove it from the updated indicator
			# Removal from the cache is implemented automatically by the cache object
            if (self.cache.currSize() == self.cache.size()):
                self.updated_indicator.remove(self.cache.get_tail())
            self.cache[key] = key
            if (use_indicator):
                self.updated_indicator.add(key)
                self.estimate_fnr_fpr () # Update the estimates of fpr and fnr, and check if it's time to send an update
            return True
            

    def get_indication (self, key):
        """
        Query the (stale) indicator of this DS
        """
        return (key in self.stale_indicator)

    def send_update (self):
        # self.updated_indicator.reset_delta_cntrs ()
        self.stale_indicator = self.updated_indicator.gen_SimpleBloomFilter ()

    def update_FN_mr(self):
        """
        update the miss-rate estimate of speculative accesses
        done using an exponential moving average
        """
        self.FN_mr_estimate.append(float(self.fn_events_cnt[-1]) / self.estimation_window)
        self.FN_mr_cur.append ( exponential_window (self.FN_mr_cur[-1], self.FN_mr_estimate[-1], self.window_alpha))  
        self.fn_events_cnt.append(0)
        
    def update_mr(self):
        """
        update the miss-rate estimate
        done using an exponential moving average.
        Used by False-Negative-Oblivious strategeis, such as the algorithms in the paper "Access Strategies for Network Caching."
        """
        self.mr_estimate.append(float(self.fp_events_cnt[-1]) / self.estimation_window)
        self.mr_cur.append (exponential_window (self.mr_cur[-1], self.mr_estimate[-1], self.window_alpha) )
        self.fp_events_cnt.append(0)
        
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
        return self.mr_cur[-1]

    def get_FN_mr(self):
        """
        get the current miss-rate estimate of speculative accesses (accesses to this DS despite a negative ind')
        """
        return self.FN_mr_cur[-1]

    def print_cache(self, head = 5):
        """
        test to see if key is in the cache
        """
        for i in itertools.islice(self.cache.dli(),head):
            print (i.key)
    

    def estimate_fnr_fpr (self):
        """
        Estimates the fnr and fpr, based on Theorems 3 and 4 in the paper: "False Rate Analysis of Bloom Filter Replicas in Distributed Systems".
        The new values are written to self.fnr_fpr, where self.fnr_fpr[0] is the fnr, and self.fnr_fpr[1] is the fpr

        """
        updated_sbf = self.updated_indicator.gen_SimpleBloomFilter ()
        Delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)), sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array))]
        B1_up = sum (updated_sbf.array) # Num of bits set in the updated indicator
        B1_st = sum (self.stale_indicator.array)
        #self.fnr_fpr = [1 - pow ( (B1-Delta[1]) / B1, self.hash_count), pow ( (B1 + Delta[0] - Delta[1])/self.BF_size, self.hash_count)]
        self.fnr = 1 - pow ( (B1_up-Delta[1]) / B1_up, self.hash_count)
        self.fpr = pow ( B1_st / self.BF_size, self.hash_count)
        #delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)) / self.BF_size, sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array)) / self.BF_size]
        #tmp = (1 - delta[1] - delta[0]) #* P1n
        #self.fnr_fpr[0] = pow (delta[1] + tmp, self.hash_count) - pow (tmp, self.hash_count) 

        if (self.verbose  == 2):
            print ('B1 = ', B1, 'Delta = ', Delta, 'fnr_fpr = ', self.fnr_fpr)
        
        
        #print ('delta0 = ', sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)) / self.BF_size, 'delta1 = ', sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array)) / self.BF_size, 'P1n = ', self.P1n, 'P1nk = ', self.P1nk, 'k = ', self.hash_count, 'fnr_fpr = ', self.fnr_fpr)
        if (self.fnr > self.max_fnr or self.fpr > self.max_fpr): # either the fpr or the fnr is too high - need to send update
            if (self.verbose  == 2):
                print ('sending update')
            self.send_update ()
            #self.fnr_fpr = [0, self.stale_indicator.get_designed_fpr()] # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr
            self.fnr = 0
            self.fpr = pow ( B1_up / self.BF_size, self.hash_count) # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr

        # Old version, based on fpr_fnr_in_dist_replicas
        # delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)) / self.BF_size, sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array)) / self.BF_size]
        # self.fnr_fpr = [self.P1nk - pow (self.P1n - delta[1], self.hash_count), pow (self.P1n + delta[0] - delta[1], self.hash_count)]
        # #print ('delta0 = ', sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)) / self.BF_size, 'delta1 = ', sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array)) / self.BF_size, 'P1n = ', self.P1n, 'P1nk = ', self.P1nk, 'k = ', self.hash_count, 'fnr_fpr = ', self.fnr_fpr)
        # if (self.fnr_fpr[0] > self.max_fnr or self.fnr_fpr[1] > self.max_fpr): # either the fpr or the fnr is too high - need to send update
        #     print ('sending update')
        #     exit ()
        #     self.send_update ()
        #     self.fnr_fpr = [0, self.stale_indicator.get_designed_fpr()] # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr
