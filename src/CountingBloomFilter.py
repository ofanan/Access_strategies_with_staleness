import numpy as np
import mmh3
import copy
import SimpleBloomFilter

class CountingBloomFilter(object):
    
    def __init__(self, size = 8000, hash_count = 5, max_array_val = 255):
        """
        Returns a CountingBloomFilter object with the following attributes:
            size:           the number of counters for mapping keys (default 1000)
            hash_count:     number of hash functions to use for mapping each key (default 5)
            max_array_val:  maximum value each counter can hold (default 255)
        """
        self.size               = size
        self.hash_count         = hash_count
        self.array              = np.zeros(size, dtype='uint8')
        self.max_array_val      = max_array_val
        self.num_of_cntrs_set   = 0 # number of cntrs set since last update was sent 
        self.num_of_cntrs_reset = 0 # number of cntrs set since last update was sent
        self.num_of_sets_between_updates    = 200 
        self.num_of_resets_between_updates  = 200
        self.insertions_since_last_update   = 0 # number of insertions since last update was sent
 
    def reset_delta_cntrs (self):
        self.num_of_cntrs_set               = 0 # number of cntrs set since last update was sent 
        self.num_of_cntrs_reset             = 0 # number of cntrs set since last update was sent        
        self.insertions_since_last_update   = 0


    def add(self, key):
        """
        adds a key to the Bloom filter
        updates all the hashes corresponding to the new key: increases each by 1 (no more than allowed maximum)
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] < self.max_array_val:
                self.insertions_since_last_update += 1
                self.array[entry] += 1
                if (self.array[entry] == 1):
                    self.num_of_cntrs_set += 1


    def remove(self, key):
        """
        removes a key from the Bloom filter
        updates all the hashes corresponding to the key: decreases each by 1 (no less than 0)
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] > 0:
                self.array[entry] -= 1
                if (self.array[entry] == 0):
                    self.num_of_cntrs_reset += 1

    def __contains__(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] == 0:
                return False
        return True

    def consider_send_update (self):
        if (self.num_of_cntrs_set >=  self.num_of_sets_between_updates or self.num_of_cntrs_reset >=  self.num_of_resets_between_updates):
            return True
        return False

    def gen_SimpleBloomFilter (self):
        """
        Returns a simple BF, which is the compression of this counting BF: each entry is True iff the respective entry in the CBF is > 0
        """
        sbf = SimpleBloomFilter.SimpleBloomFilter (size=self.size, hash_count = self.hash_count)
        for i in range (self.size):
            if (self.array[i] > 0):
                sbf.array[i] = True
        return sbf

    def lookup(self, key):
        """
        checks if all hash functions corresponding to key are strictly positive
        """
        for seed in range(self.hash_count):
            entry = mmh3.hash(key, seed) % self.size
            if self.array[entry] == 0:
                return False
        return True
        
    def get_fpr(self):
        """
        return the estimate false-positive ratio
        CHECK THE FORMULA!!!
        """
        return (sum(self.array > 0) / self.size) ** s.hash_count


    def calc_deltas (self, stale_sbf):
        """
        returns an array, delta, where
        delta[0] represents the number of cntr that are reset in self (the updated BF), but set   in the input sbf (Simple BF - the stale BF)
        delta[1] represents the number of cntr that are set   in self (the updated BF), but reset in the input sbf (Simple BF - the stale BF)
        """
        updated_sbf = self.gen_SimpleBloomFilter ()
        return [sum (np.bitwise_and (~updated_sbf.array, stale_sbf.array)), sum (np.bitwise_and (updated_sbf.array, ~stale_sbf.array)) ] 

    def estimate_rho0_rho1 (self):
        """
        returns:
        rho0: the probability that the datum is not in the cache, given a negative indication 
        rho1: the probability that the datum is not in the cache, given a positive indication 
        """
