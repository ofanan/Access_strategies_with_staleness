import numpy as np
import mod_pylru
import itertools
import CountingBloomFilter as CBF
import copy
from MyConfig import get_optimal_hash_count 

class DataStore (object):
    
    def __init__(self, ID, size = 1000, bpe = 5, miss_rate_alpha = 0.1, estimation_window = 1000, miss_rate_init = 0.5, max_fnr = 0, max_fpr = 0):
        """
        Return a DataStore object with the following attributes:
            ID:                 datastore ID 
            size:               number of elements that can be stored in the datastore (default 1000)
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            estimation_window:  how often (queries) should the miss-rate estimation be updated (default 1000, same as size)
            miss_rate_alpha:    sliding window parameter for miss-rate estimation (default 0.1)
            miss_rate_init:     initial miss-rate estimation (default 0.5)
        """
        self.ID                     = ID
        self.size                   = size
        self.bpe                    = bpe
        self.BF_size                = self.bpe * self.size 
        self.hash_count             = get_optimal_hash_count (self.bpe)
        self.mr_alpha               = miss_rate_alpha
        self.estimation_window      = estimation_window
        self.mr_cur                 = [miss_rate_init]
        self.mr_win_estimate        = []
        self.mr_win_miss_cnt        = [0]
        self.FN_mr_win_estimate     = []
        self.FN_mr_win_miss_cnt     = [0]
        self.FN_mr_cur              = [miss_rate_init]
        self.access_cnt             = 0
        self.hit_cnt                = 0
        self.max_fnr                = max_fnr
        self.max_fpr                = max_fpr
        self.P1n                    = 1 - np.exp (-self.hash_count / self.bpe)
        self.P1nk                   = pow (self.P1n, self.hash_count)

        # create a counting Bloom filter
        # for documentation, see:
        #    http://www.maxburstein.com/blog/creating-a-simple-bloom-filter/
        #    https://hur.st/bloomfilter/
        #    http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        #    Bloom filter survey: https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf
        self.updated_indicator = CBF.CountingBloomFilter (size = self.BF_size, hash_count = self.hash_count)
        self.stale_indicator   = self.updated_indicator.gen_SimpleBloomFilter ()         
        # create an LRU cache
        # for documentation, see: https://pypi.org/project/pylru/
        self.cache = mod_pylru.lrucache(self.size)
        
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
                self.FN_mr_win_miss_cnt[-1] += 1
            else:
                self.mr_win_miss_cnt[-1] += 1
            return False

    def insert(self, key):
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
            self.updated_indicator.add(key)
            self.estimate_fnr_fpr () #Check if it's time to send an update
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
        self.FN_mr_win_estimate.append(float(self.FN_mr_win_miss_cnt[-1]) / self.estimation_window)
        self.FN_mr_cur.append (self.mr_alpha * (self.FN_mr_win_estimate[-1]) + (1 - self.mr_alpha) * self.FN_mr_cur[-1] )
        self.FN_mr_win_miss_cnt.append(0)
        
    def update_mr(self):
        """
        update the miss-rate estimate
        done using an exponential moving average.
        Used by False-Negative-Oblivious strategeis, such as the algorithms in the paper "Access Strategies for Network Caching."
        """
        self.mr_win_estimate.append(float(self.mr_win_miss_cnt[-1]) / self.estimation_window)
        self.mr_cur.append (self.mr_alpha * (self.mr_win_estimate[-1]) + (1 - self.mr_alpha) * self.mr_cur[-1] )
        self.mr_win_miss_cnt.append(0)
        
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
        returns an array ar, where:
        ar[0] (ar[1]) is the expected false negative rate (false negative rate).
        The calculation is based on Theorems 3 and 4 in the paper: "False Rate Analysis of Bloom Filter Replicas in Distributed Systems".
        """
        updated_sbf = self.updated_indicator.gen_SimpleBloomFilter ()
        # delta[0] (delta[1]) will hold the prob' that a concrete bit was reset (set) since the last update
        delta = [sum (np.bitwise_and (~updated_sbf.array, self.stale_indicator.array)) / self.BF_size, sum (np.bitwise_and (updated_sbf.array, ~self.stale_indicator.array)) / self.BF_size] 
        fnr_fpr = [self.P1nk - pow (self.P1n - delta[1], self.hash_count), pow (self.P1n + delta[0] - delta[1], self.hash_count)]
        if (fnr_fpr[0] > self.max_fnr or fnr_fpr[1] > self.max_fpr): # either the fpr or the fnr is too high - need to send update
            self.send_update ()
            return [0, self.stale_indicator.get_designed_fpr()] # Immediately after sending an update, the expected fnr is 0, and the expected fpr is the inherent fpr
        return fnr_fpr  
