import numpy as np
import pandas as pd
import DataStore
import Client
import candidate
import node 
import sys
import pickle
import sys
import random

# Codes for access algorithms
ALG_OPT             = 1 # Optimal access strategy (perfect indicator)
ALG_PGM             = 2 # PGM alg', detailed in Access Strategies journal paper
ALG_CHEAP           = 3 # Cheapest (CPI) strategy: in case of a positive indication, access the minimal-cost DS with positive indication 
ALG_ALL             = 4 # All (EPI) strategy: in case of positive indications, access all DSs with positive indications
ALG_KNAP            = 5 # Knapsack-based alg'. See Access Strategies papers.
ALG_POT             = 6 # Potential-based alg'. See Access Strategies papers.
ALG_PGM_FNO         = 7 # PGM alg', detailed in Access Strategies journal paper; Staleness Oblivious
ALG_PGM_FNA         = 8 # PGM alg', detailed in Access Strategies journal paper; staleness-aware
ALG_OPT_HOMO        = 100 # Optimal access strategy (perfect indicator), faster version for the case of homo' accs costs
ALG_PGM_FNO_HOMO    = 107 # PGM alg', detailed in Access Strategies journal paper; Staleness Oblivious. Faster version for the case of homo' accs costs
ALG_PGM_FNA_HOMO    = 108 # PGM alg', detailed in Access Strategies journal paper; Staleness Oblivious. Faster version for the case of homo' accs costs

# client action: updated according to what client does
# 0: no positive ind , 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer missp, 4: no pos ind, pay missp
"""
key is an integer
"""

class Simulator(object):

    # init a list of empty DSs
    def init_DS_list(self):
        return [DataStore.DataStore(ID = i, size = self.DS_size, bpe = self.bpe, estimation_window = self.estimation_window, verbose = self.verbose) for i in range(self.num_of_DSs)]
            
    def init_client_list(self):
        self.client_list = [Client.Client(ID = i, num_of_DSs = self.num_of_DSs, estimation_window = self.estimation_window, verbose = self.verbose, 
        use_redundan_coef = self.use_redundan_coef, k_loc = self.k_loc) for i in range(self.num_of_clients)]
    
    def __init__(self, alg_mode, DS_insert_mode, req_df, client_DS_cost, missp, k_loc, DS_size = 1000, bpe = 15, rand_seed = 42, use_redundan_coef = False, verbose = 0):
        """
        Return a Simulator object with the following attributes:
            alg_mode:           mode of client: defined by macros above
            DS_insert_mode:     mode of DS insertion (1: fix, 2: distributed, 3: ego)
            req_df:             array of keys of requests. each key is a string
            client_DS_cost:     2D array of costs. entry (i,j) is the cost from client i to DS j
            missp:               miss penalty
            k_loc:              number of DSs a missed key is inserted to
            DS_size:            size of DS (default 1000)
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            alpha:              weight for convex combination of dist-bw for calculating costs (default 0.5)
        """
        self.missp          = missp
        self.k_loc          = k_loc
        self.k_loc_min_1    = k_loc-1 # To speedup some computation, we "precompute" only once k_loc - 1
        self.DS_size        = DS_size
        self.bpe            = bpe
        self.rand_seed      = rand_seed
        self.DS_insert_mode = DS_insert_mode
        if (self.DS_insert_mode != 1):
            print ('sorry, currently only fix insert mode (1) is supported')
            exit ()

        self.num_of_clients     = client_DS_cost.shape[0]
        self.num_of_DSs         = client_DS_cost.shape[1]
        self.client_DS_cost     = client_DS_cost # client_DS_cost(i,j) will hold the access cost for client i accessing DS j
        self.estimation_window  = self.DS_size # window for parameters' estimation 
        self.verbose            = verbose # Used for debug / analysis: a higher level verbose prints more msgs to the Screen / output file.
        self.DS_list            = self.init_DS_list() #DS_list is the list of DSs
        self.mr_of_DS           = np.zeros(self.num_of_DSs) # mr_of_DS[i] will hold the estimated miss rate of DS i 
        self.req_df             = req_df        
        self.use_redundan_coef  = use_redundan_coef
        self.req_cnt            = -1
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs , dtype='uint') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.leaf_of_DS         = np.array(np.floor(np.log2(self.client_DS_cost))).astype('uint8') # lg_client_DS_cost(i,j) will hold the lg2 of access cost for client i accessing DS j
        self.cur_pos_DS_list    = [] #np.array (0, dtype = 'uint8') #list of the DSs with pos' ind' (positive indication) for the current request
        self.q_estimation       = np.zeros (self.num_of_DSs , dtype='uint') #q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.window_alhpa       = 0.25 # window's alpha parameter for estimated parameters       
        
        self.alg_mode           = alg_mode
        if (self.DS_costs_are_homo()):
            if (self.alg_mode == ALG_PGM_FNO):
                self.alg_mode = ALG_PGM_FNO_HOMO
            if (self.alg_mode == ALG_PGM_FNA):
                self.alg_mode = ALG_PGM_FNA_HOMO
            elif (self.alg_mode == ALG_OPT):
                self.alg_mode = ALG_OPT_HOMO
        self.init_client_list ()

        # Statistical parameters (collected / estimated at run time)
        self.total_cost         = float(0)
        self.access_cnt         = 0
        self.hit_cnt            = 0
        self.total_access_cost  = 0
        self.high_cost_mp_cnt   = 0 # counts the misses for cases where accessing DSs was too costly, so the alg' decided to access directly the mem
        self.comp_miss_cnt      = 0
        self.non_comp_miss_cnt  = 0
        self.speculate_accs_cnt = 0 # num of speculative accss, that is, accesses to a DS despite a miss indication
        self.speculate_hit_cnt  = 0 # num of hits among speculative accss
        self.FN_miss_cnt        = 0 # num of misses happened due to FN event

        # Debug / verbose variables
        if (self.verbose == 1):
            self.num_DS_accessed = float(0) #Currently unused
            self.avg_DS_accessed_per_req = float(0)
        if (self.verbose == 3):
            self.debug_file = open ("../res/fna.txt", "w", buffering=1)


    def DS_costs_are_homo (self):
        """
        returns true iff all the DS costs, of all clients, are identical
        """
        for i in range (self.num_of_clients):
            for j in range (self.num_of_DSs):
                if (self.client_DS_cost[i][j] != self.client_DS_cost[0][0]):
                    return False 
        return True

    def PGM_FNA_partition (self):
        """
        Performs the partition stage in the PGM-Staeleness-Aware alg'. 
        In a FNA (aka stale-aware) alg', the candidate DSs are all the DSs (even those with negative ind'), and
        therefore it's possible to perform the partition stage only once, instead of re-run it for each request.
        """

        self.DSs_in_leaf = [] # DSs_in_leaf[i] will hold the 2D list of DSs of client i, as described below
        self.num_of_leaves = np.zeros (self.num_of_clients, dtype = 'uint8') #num_of_leaves[i] will hold the # of leaves in PGM alg' when the client is i
        for client_id in range (self.num_of_clients):
            self.num_of_leaves[client_id] = np.max (np.take(self.leaf_of_DS [client_id], range (self.num_of_DSs))) + 1 
            DSs_in_leaf = [[]] # For the current client_id, DSs_in_leaf[j] will hold the list of DSs which belong leaf j, that is, the IDs of all the DSs with access in [2^j, 2^{j+1})
            for leaf_num in range (self.num_of_leaves[client_id]):
                DSs_in_leaf.append ([])
            for ds_id in (range(self.num_of_DSs)):
                DSs_in_leaf[self.leaf_of_DS[client_id][ds_id]].append(ds_id)
            self.DSs_in_leaf.append(DSs_in_leaf)



    def gather_statistics(self):
        """
        Accumulates and organizes the stat collected during the sim' run.
        This func' is usually called once at the end of each run of the python_simulator.
        """
        self.total_access_cost  = np.sum ( [client.total_access_cost for client in self.client_list ] ) 
        self.hit_cnt            = np.sum ( [client.hit_cnt for client in self.client_list ] )
        self.hit_ratio          = float(self.hit_cnt) / self.req_cnt
        self.non_comp_miss_cnt  = np.sum( [client.non_comp_miss_cnt for client in self.client_list ] )
        self.comp_miss_cnt      = np.sum( [client.comp_miss_cnt for client in self.client_list ] )
        self.high_cost_mp_cnt   = np.sum( [client.high_cost_mp_cnt for client in self.client_list ] )
        self.total_cost         = self.total_access_cost + self.missp * (self.comp_miss_cnt + self.non_comp_miss_cnt + self.high_cost_mp_cnt)
        self.avg_DS_hit_ratio   = np.average ([DS.get_hr() for DS in self.DS_list])
        print ('alg_mode = %d, tot_cost=%.2f, tot_access_cost= %.2f, hit_ratio = %.2f, non_comp_miss_cnt = %d, comp_miss_cnt = %d' % 
                 (self.alg_mode, self.total_cost, self.total_access_cost, self.hit_ratio, self.non_comp_miss_cnt, self.comp_miss_cnt)        )
    

    def run_trace_opt_hetro (self):
        """
        Run a full trace as Opt access strat' when the DS costs are heterogneous
        """
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            self.req_cnt += 1
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            # get the list of datastores holding the request
            true_answer_DS_list = np.array([DS_id for DS_id in range(self.num_of_DSs) if (self.cur_req.key in self.DS_list[DS_id])])

            if true_answer_DS_list.size == 0: # Request is indeed not found in any DS
                self.client_list[self.client_id].comp_miss_cnt += 1
                self.insert_key_to_DSs_without_indicator () # Opt doesn't really use indicators - it "knows" the actual contents of the DSs
            else: 
                # find the cheapest DS holding the request
                access_DS_id = true_answer_DS_list[np.argmin( np.take( self.client_DS_cost[self.client_id] , true_answer_DS_list ) )]
                # We assume here that the cost of every DS < missp
                # update variables
                self.client_list[self.client_id].total_access_cost += self.client_DS_cost[self.client_id][access_DS_id]
                if (self.verbose == 1):
                    self.client_list[self.client_id].add_DS_accessed(self.cur_req.req_id, [access_DS_id])
                self.client_list[self.client_id].access_cnt += 1
                # perform access. we know it will be successful
                self.DS_list[access_DS_id].access(self.cur_req.key)
                self.client_list[self.client_id].hit_cnt += 1

    def run_trace_opt_homo (self):
        """
        Run a full trace as Opt access strat' when all the DS costs are 1 
        """
        self.comp_miss_cnt  = 0
        self.hit_cnt        = 0
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            self.req_cnt += 1
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            # get the list of datastores holding the request
            true_answer_DS_list = np.array([DS_id for DS_id in range(self.num_of_DSs) if (self.cur_req.key in self.DS_list[DS_id])])

            if true_answer_DS_list.size == 0: # Request is indeed not found in any DS
                self.comp_miss_cnt += 1
                self.insert_key_to_DSs_without_indicator () # Opt doesn't really use indicators - it "knows" the actual contents of the DSs
            else: 
                self.DS_list [self.cur_req['%d'%(random.randint (0, true_answer_DS_list.size-1))]].access(self.cur_req.key) # Access a single DS, chosen "randomly" among the 
                self.hit_cnt += 1
        self.hit_ratio               = float(self.hit_cnt) / self.req_cnt
        print ('alg_mode = %d, tot_cost=%.2f, tot_access_cost= %.2f, hit_ratio = %.2f, comp_miss_cnt = %d' % 
                 (self.alg_mode, self.hit_cnt + self.missp * self.comp_miss_cnt, self.hit_cnt, self.hit_ratio, self.comp_miss_cnt)        )

    def run_trace_pgm_fno_hetro (self):
        """
        Run a full trace where the access strat' is the PGM, as proposed in the journal paper "Access srategies for Network Caching".
        """
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            self.req_cnt += 1
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            self.cur_pos_DS_list = np.array ([DS.ID for DS in self.DS_list if (self.cur_req.key in DS.stale_indicator) ]) # self.cur_pos_DS_list <- list of DSs with positive indications
            if (len(self.cur_pos_DS_list) == 0): # No positive indications --> FNO alg' has a miss
                self.handle_miss ()
                continue        
            self.estimate_mr_by_history () # Update the estimated miss rates of the DSs; the updated miss rates of DS i will be written to mr_of_DS[i]   
            self.access_pgm_fno_hetro ()

    def run_trace_pgm_fna_hetro (self):
        """
        Run a full trace where the access strat' is PGM, at its "Staleness-Aware" version. 
        The simplified, False-Negative-Obvliious alg' of PGM, is detailed in the journal paper "Access srategies for Network Caching".
        """
        self.PGM_FNA_partition ()
        self.indications = np.array (range (self.num_of_DSs), dtype = 'bool') 
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            self.req_cnt += 1
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            for i in range (self.num_of_DSs):
                self.indications[i] = True if (self.cur_req.key in self.DS_list[i].stale_indicator) else False #self.indication[i] holds the indication of DS i for the cur request
            self.access_pgm_fna_hetro ()


    def run_trace_pgm_fno_homo (self):
        """
        Run a full trace where the access strat' is the "potential" alg' from the paper "Access Strategies in Network Caching", 
        for the special case where the access costs are homogeneous (all of them are 1). 
        This alg' is staleness-oblivious
        """
        for req_id in range(self.req_df.shape[0]): # for each request in the trace...
            self.req_cnt += 1 
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            self.cur_pos_DS_list = np.array ([DS.ID for DS in self.DS_list if (self.cur_req.key in DS.stale_indicator) ]) # self.cur_pos_DS_list <- list of DSs with positive indications
            if (len(self.cur_pos_DS_list) == 0): # No positive indications --> FNO alg' has a miss
                self.handle_miss ()
                continue        
            self.estimate_mr_by_history () # Update the estimated miss rates of the DSs; the updated miss rates of DS i will be written to mr_of_DS[i]   
            self.find_homo_sol (sorted (self.cur_pos_DS_list, key=self.mr_of_DS.__getitem__)) # Homogeneous access, where the candidates are only DSs with positive ind'. 
            # The func' find_homo_sol writes the suggested sol to self.sol
            
            if (len(self.sol) == 0): # the alg' decided not to access any DS
                self.handle_miss ()
                return

            # Now we know that the alg' decided to access at least one DS
            self.client_list[self.client_id].total_access_cost += len(self.sol)

            # perform access. the function access() returns True if successful, and False otherwise
            accesses = np.array([self.DS_list[DS_id].access(self.cur_req.key) for DS_id in self.sol])
            if any(accesses):   #hit
                self.client_list[self.client_id].hit_cnt += 1
            else:               # Miss
                self.handle_miss ()

    def run_trace_pgm_fna_homo (self):
        """
        Run a full trace where the access strat' is the "potential" alg' from the paper "Access Strategies in Network Caching", 
        for the special case where the access costs are homogeneous (all of them are 1). 
        This alg' is staleness-aware
        """
        self.indications = np.array (range (self.num_of_DSs), dtype = 'bool') 
        self.total_access_cost = 0
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            self.req_cnt += 1 
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.client_id = self.cur_req.client_id
            for i in range (self.num_of_DSs):
                self.indications[i] = True if (self.cur_req.key in self.DS_list[i].stale_indicator) else False #self.indication[i] holds the indication of DS i for the cur request
            self.mr_of_DS           = self.client_list [self.client_id].get_mr (self.indications) # Get the probability that the requested item is in DS i, given to the concrete indication of its indicator
            self.find_homo_sol (sorted (range (self.num_of_DSs), key=self.mr_of_DS.__getitem__)) #Homogeneous access, where the candidates are only DSs with positive ind'
            if (len(self.sol) == 0): # the alg' decided to not access any DS
                self.handle_miss ()
                continue
            self.client_list[self.client_id].total_access_cost += len(self.sol)
            hit = False
            for DS_id in self.sol:
                if (not (self.indications[DS_id])): #A speculative accs 
                    self.speculate_accs_cost += 1
                if (self.DS_list[DS_id].access(self.cur_req.key)): # hit
                    if (not (hit) and (not (self.indications[DS_id]))): # this is the first hit; for each speculative req, we want to count at most a single hit 
                        self.speculate_hit_cnt += 1
                    hit = True
                    self.client_list [self.client_id].fnr[DS_id] = self.DS_list[DS_id].fnr;  
                    self.client_list [self.client_id].fpr[DS_id] = self.DS_list[DS_id].fpr;  
            if (hit):   
                self.client_list[self.client_id].hit_cnt += 1
            else:               # Miss
                self.handle_miss ()


    def run_simulator (self):
        """
        Run a simulation, gather statistics and prints outputs
        """
        np.random.seed(self.rand_seed)
        num_of_req = self.req_df.shape[0]
        print ('running alg mode ', self.alg_mode)
        if self.alg_mode == ALG_OPT:
            self.run_trace_opt_hetro ()
            self.gather_statistics ()
        elif self.alg_mode == ALG_OPT_HOMO:
            self.run_trace_opt_homo ()
        elif self.alg_mode == ALG_PGM_FNO:
            self.run_trace_pgm_fno_hetro ()
            self.gather_statistics ()
            print ('FN miss cnt = ', self.FN_miss_cnt)
        elif (self.alg_mode == ALG_PGM_FNO_HOMO):
            self.run_trace_pgm_fno_homo ()
            self.gather_statistics ()
            print ('FN miss cnt = ', self.FN_miss_cnt)
        elif self.alg_mode == ALG_PGM_FNA:
            self.run_trace_pgm_fna_hetro ()
            self.gather_statistics()
            print ('num of spec accs = ', self.speculate_accs_cnt, ', num of spec hits = ', self.speculate_hit_cnt)
        elif self.alg_mode == ALG_PGM_FNA_HOMO:
            self.run_trace_pgm_fna_homo ()
            self.gather_statistics()
            print ('num of spec accs = ', self.speculate_accs_cnt, ', num of spec hits = ', self.speculate_hit_cnt)
        else: 
            print ('Wrong alg_mode: ', self.alg_mode)

        
    # Updates self.cur_pos_DS_list, so that it will hold an np.array of the IDs of the DSs with positive ind'
    def get_indications(self):
        self.cur_pos_DS_list = np.array ([DS.ID for DS in self.DS_list if (self.cur_req.key in DS.stale_indicator) ])
        
    def estimate_mr_by_history (self):
        """
        Update the estimated miss rate of each DS, based on the history.
        This func is used ONLY BY FNO alg', as it assumes that a DS is accessed only upon a positive ind'. 
        """
        self.mr_of_DS = np.array([DS.mr_cur[-1] for DS in self.DS_list]) # For each 1 <= i<= n, Copy the miss rate estimation of DS i to mr_of_DS(i)

    def handle_compulsory_miss (self):
        """
        Called upon a compulsory miss, namely, a fail to retreive a request from any DS, while the request is indeed not stored in any of the DSs.
        The func' increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        self.client_list[self.client_id].comp_miss_cnt += 1
        self.insert_key_to_DSs ()

    def handle_non_compulsory_miss (self):
        """
        Called upon a non-compulsory miss, namely, a fail to retreive a request from any DS, while the request is actually stored in at least one DS.
        The func' increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        self.client_list[self.client_id].non_comp_miss_cnt += 1
        self.insert_key_to_DSs ()
        if (self.alg_mode == ALG_PGM_FNO or self.alg_mode == ALG_PGM_FNO_HOMO):
            self.FN_miss_cnt += 1

    def handle_miss (self):
        """
        Called upon a miss. Check whether the miss is compulsory or not. Increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        if (self.is_compulsory_miss()):
            self.handle_compulsory_miss ()
        else:
            self.handle_non_compulsory_miss ()

    def insert_key_to_closest_DS(self, req):
        """
        Currently unused
        """
        # check to see if one needs to insert key to closest cache too
        if self.DS_insert_mode == 2:
            self.DS_list[self.client_id].insert(req.key)

    def insert_key_to_random_DSs(self, req):
        # use the first location as the random DS to insert to.
        self.DS_list[req['0']].insert(req.key)

    def insert_key_to_DSs_without_indicator (self):
        """
        insert key to all k_loc DSs, which are defined by the input (parsed) trace
        Do not use indicator. Used for Opt, which doesn't need indicators.
        """
        for i in range(self.k_loc):
            self.DS_list[self.cur_req['%d'%i]].insert (self.cur_req.key, use_indicator = False) 
            
 
    def insert_key_to_DSs(self):
        """
        insert key to all k_loc DSs, which are defined by the input (parsed) trace
        """
        for i in range(self.k_loc):
            self.DS_list[self.cur_req['%d'%i]].insert (self.cur_req.key) 
            
    def is_compulsory_miss (self):
        """
        Returns true iff the access is compulsory miss, namely, the requested datum is indeed not found in any DS.
        """
        return (np.array([DS_id for DS_id in range(self.num_of_DSs) if (self.cur_req.key in self.DS_list[DS_id])]).size == 0) # cur_req is indeed not stored in any DS 

    def find_homo_sol (self, sorted_list_of_DSs):
        """
        Find the best solution when costs are homogeneous. The alg' is based on the "potential" alg' from the paper "Access Strategies in Network Caching". 
        """
        # By default, the proposal is to access no DS, resulting in 0 access cost, and an expected miss ratio of 1
        cur_accs_cost           = 0 
        cur_expected_miss_ratio = 1
        cur_expected_total_cost = self.missp
        self.sol                = []
        for DS_id in sorted_list_of_DSs:
            nxt_accs_cost           = cur_accs_cost + 1
            nxt_expected_miss_ratio = cur_expected_miss_ratio * self.mr_of_DS[DS_id] 
            nxt_expected_total_cost = nxt_accs_cost + nxt_expected_miss_ratio * self.missp  
            if (nxt_expected_total_cost < cur_expected_total_cost): # Adding this DS indeed decreases the expected total cost
                cur_accs_cost           = nxt_accs_cost
                cur_expected_miss_ratio = nxt_expected_miss_ratio
                cur_expected_total_cost = nxt_expected_total_cost
                self.sol.append (DS_id)
            else: # Adding more DSs may only increase the total cost
                break
    
        
    def access_pgm_fno_hetro (self):
        """
        The PGM FNO (false negative oblivious) alg' detailed in the paper: Access Strategies for Network Caching, Journal verison.
        """ 
        # Now we know that there exists at least one positive indication
        self.cur_pos_DS_list = [int(i) for i in self.cur_pos_DS_list] # cast cur_pos_DS_list to int

        # Partition stage
        ###############################################################################################################
        # leaf_of_DS (i,j) holds the leaf to which DS with cost (i,j) belongs, that is, log_2 (DS(i,j))

        # leaves_of_DSs_w_pos_ind will hold the leaves of the DSs with pos' ind'
        cur_num_of_leaves = np.max (np.take(self.leaf_of_DS[self.client_id], self.cur_pos_DS_list)) + 1

        # DSs_in_leaf[j] will hold the list of DSs which belong leaf j, that is, the IDs of all the DSs with access in [2^j, 2^{j+1})
        DSs_in_leaf = [[]]
        for leaf_num in range (cur_num_of_leaves):
            DSs_in_leaf.append ([])
        for ds in (self.cur_pos_DS_list):
            DSs_in_leaf[self.leaf_of_DS[self.client_id][ds]].append(ds)

        # Generate stage
        ###############################################################################################################
        # leaf[j] will hold the list of candidate DSs of V^0_j in the binary tree
        leaf = [[]]
        for leaf_num in range (cur_num_of_leaves-1): # Append additional cur_num_of_leaves-1 leaves
            leaf.append ([])

        for leaf_num in range (cur_num_of_leaves):

            # df_of_DSs_in_cur_leaf will hold the IDs, miss rates and access costs of the DSs in the current leaf
            num_of_DSs_in_cur_leaf = len(DSs_in_leaf[leaf_num])
            df_of_DSs_in_cur_leaf = pd.DataFrame({
                'DS ID': DSs_in_leaf[leaf_num],
                'mr': np.take(self.mr_of_DS, DSs_in_leaf[leaf_num]), #miss rate
                'ac': np.take(self.client_DS_cost[self.client_id], DSs_in_leaf[leaf_num]) #access cost
            })


            df_of_DSs_in_cur_leaf.sort_values(by=['mr'], inplace=True) # sort the DSs in non-dec. order of miss rate


            leaf[leaf_num].append(candidate.candidate ([], 1, 0)) # Insert the empty set to the leaf
            cur_mr = 1
            cur_ac = 0

            # For each prefix_len \in {1 ... number of DSs in the current leaf},
            # insert the prefix at this prefix_len to the current leaf
            for pref_len in range (1, num_of_DSs_in_cur_leaf+1):
                cur_mr *= df_of_DSs_in_cur_leaf.iloc[pref_len - 1]['mr']
                cur_ac += df_of_DSs_in_cur_leaf.iloc[pref_len - 1]['ac']
                leaf[leaf_num].append(candidate.candidate(df_of_DSs_in_cur_leaf.iloc[range(pref_len)]['DS ID'], cur_mr, cur_ac))

        # Merge stage
        ###############################################################################################################
        r = np.ceil(np.log2(self.missp)).astype('uint8')
        num_of_lvls = (np.ceil(np.log2 (cur_num_of_leaves))).astype('uint8') + 1
        if (num_of_lvls == 1): # Only 1 leaf --> nothing to merge. The candidate full solutions will be merely those in this single leaf
            cur_lvl_node = leaf
        else:
            prev_lvl_nodes = leaf
            num_of_nodes_in_prev_lvl = cur_num_of_leaves
            num_of_nodes_in_cur_lvl = np.ceil (cur_num_of_leaves / 2).astype('uint8')
            for lvl in range (1, num_of_lvls):
                cur_lvl_node = [None]*num_of_nodes_in_cur_lvl
                for j in range (num_of_nodes_in_cur_lvl):
                    if (2*(j+1) > num_of_nodes_in_prev_lvl): # handle edge case, when the merge tree isn't a full binary tree
                        cur_lvl_node[j] = prev_lvl_nodes[2*j]
                    else:
                        cur_lvl_node[j] = node.merge(prev_lvl_nodes[2*j], prev_lvl_nodes[2*j+1], r, self.missp)
                num_of_nodes_in_prev_lvl = num_of_nodes_in_cur_lvl
                num_of_nodes_in_cur_lvl = (np.ceil(num_of_nodes_in_cur_lvl / 2)).astype('uint8')
                prev_lvl_nodes = cur_lvl_node

        min_final_candidate_phi = self.missp + 1 # Will hold the total cost among by all final sols checked so far
        for final_candidate in cur_lvl_node[0]:  # for each of the candidate full solutions
            final_candidate_phi = final_candidate.phi(self.missp)
            if (final_candidate_phi < min_final_candidate_phi): # if this sol' is cheaper than any other sol' found so far', take this new sol'
                final_sol = final_candidate
                min_final_candidate_phi = final_candidate_phi

        if (len(final_sol.DSs_IDs) == 0): # the alg' decided to not access any DS
            self.handle_miss ()
            return

        # Now we know that the alg' decided to access at least one DS
        # Add the costs and IDs of the selected DSs to the statistics
        self.client_list[self.client_id].total_access_cost += final_sol.ac
        if (self.verbose == 1):
            self.client_list[self.client_id].add_DS_accessed(self.cur_req.req_id, final_sol.DSs_IDs)
        self.client_list[self.client_id].access_cnt += 1

        # perform access. the function access() returns True if successful, and False otherwise
        # if (self.verbose == 2):
        #     print ('req cnt = ', self.req_cnt, 'pos ind = ', self.cur_pos_DS_list, 'mr = ', self.mr_of_DS, 'accss = ', final_sol.DSs_IDs)
        accesses = np.array([self.DS_list[DS_id].access(self.cur_req.key) for DS_id in final_sol.DSs_IDs])
        if any(accesses):   #hit
            self.client_list[self.client_id].hit_cnt += 1
        else:               # Miss
            self.handle_miss ()
        return


    def access_pgm_fna_hetro (self):

        req                     = self.cur_req
        self.mr_of_DS           = self.client_list [self.client_id].get_mr (self.indications) # Get the probability that the requested item is in DS i, given to the concrete indication of its indicator

        # Partition stage is done once, statically, based on the DSs' costs
        ###############################################################################################################

        # Generate stage
        ###############################################################################################################
        cur_num_of_leaves = self.num_of_leaves[self.client_id] 
        DSs_in_leaf = self.DSs_in_leaf[self.client_id]

        # leaf[j] will hold the list of candidate DSs of V^0_j in the binary tree
        leaf = [[]]
        for leaf_num in range (cur_num_of_leaves-1): # Append additional cur_num_of_leaves-1 leaves
            leaf.append ([])

        for leaf_num in range (cur_num_of_leaves):

            # df_of_DSs_in_cur_leaf will hold the IDs, miss rates and access costs of the DSs in the current leaf
            num_of_DSs_in_cur_leaf = len(DSs_in_leaf[leaf_num])
            df_of_DSs_in_cur_leaf = pd.DataFrame({
                'DS ID': DSs_in_leaf[leaf_num],
                'mr': np.take (self.mr_of_DS, DSs_in_leaf[leaf_num]), #miss rate
                'ac': np.take (self.client_DS_cost[self.client_id], DSs_in_leaf[leaf_num]) #access cost
            })


            df_of_DSs_in_cur_leaf.sort_values(by=['mr'], inplace=True) # sort the DSs in non-dec. order of miss rate


            leaf[leaf_num].append(candidate.candidate ([], 1, 0)) # Insert the empty set to the leaf
            cur_mr = 1
            cur_ac = 0

            # For each prefix_len \in {1 ... number of DSs in the current leaf},
            # insert the prefix at this prefix_len to the current leaf
            for pref_len in range (1, num_of_DSs_in_cur_leaf+1):
                cur_mr *= df_of_DSs_in_cur_leaf.iloc[pref_len - 1]['mr']
                cur_ac += df_of_DSs_in_cur_leaf.iloc[pref_len - 1]['ac']
                leaf[leaf_num].append(candidate.candidate(df_of_DSs_in_cur_leaf.iloc[range(pref_len)]['DS ID'], cur_mr, cur_ac))

        # Merge stage
        ###############################################################################################################
        r = np.ceil(np.log2(self.missp)).astype('uint8')
        num_of_lvls = (np.ceil(np.log2 (cur_num_of_leaves))).astype('uint8') + 1
        if (num_of_lvls == 1): # Only 1 leaf --> nothing to merge. The candidate full solutions will be merely those in this single leaf
            cur_lvl_node = leaf
        else:
            prev_lvl_nodes = leaf
            num_of_nodes_in_prev_lvl = cur_num_of_leaves
            num_of_nodes_in_cur_lvl = np.ceil (cur_num_of_leaves / 2).astype('uint8')
            for lvl in range (1, num_of_lvls):
                cur_lvl_node = [None]*num_of_nodes_in_cur_lvl
                for j in range (num_of_nodes_in_cur_lvl):
                    if (2*(j+1) > num_of_nodes_in_prev_lvl): # handle edge case, when the merge tree isn't a full binary tree
                        cur_lvl_node[j] = prev_lvl_nodes[2*j]
                    else:
                        cur_lvl_node[j] = node.merge(prev_lvl_nodes[2*j], prev_lvl_nodes[2*j+1], r, self.missp)
                num_of_nodes_in_prev_lvl = num_of_nodes_in_cur_lvl
                num_of_nodes_in_cur_lvl = (np.ceil(num_of_nodes_in_cur_lvl / 2)).astype('uint8')
                prev_lvl_nodes = cur_lvl_node

        min_final_candidate_phi = self.missp + 1 # Will hold the total cost among by all final sols checked so far
        for final_candidate in cur_lvl_node[0]:  # for each of the candidate full solutions
            final_candidate_phi = final_candidate.phi(self.missp)
            if (final_candidate_phi < min_final_candidate_phi): # if this sol' is cheaper than any other sol' found so far', take this new sol'
                final_sol = final_candidate
                min_final_candidate_phi = final_candidate_phi

        if (len(final_sol.DSs_IDs) == 0): # the alg' decided to not access any DS
            self.handle_miss ()
            return

        # Now we know that the alg' decided to access at least one DS
        # Add the costs and IDs of the selected DSs to the statistics
        self.client_list[self.client_id].total_access_cost += final_sol.ac
        if (self.verbose == 1):
            self.client_list[self.client_id].add_DS_accessed(self.cur_req.req_id, final_sol.DSs_IDs)
        self.client_list[self.client_id].access_cnt += 1

        # perform access
        self.sol = final_sol.DSs_IDs
        self.perform_fna_access ()
        hit = False
        for DS_id in final_sol.DSs_IDs:
            if (not (self.indications[DS_id])): #A speculative accs 
                self.speculate_accs_cost += self.client_DS_cost [self.client_id][DS_id]
            if (self.DS_list[DS_id].access(self.cur_req.key)): # hit
                if (not (hit) and (not (self.indications[DS_id]))): # this is the first hit; for each speculative req, we want to count at most a single hit 
                    self.speculate_hit_cnt += 1
                hit = True
                self.client_list [self.client_id].fnr[DS_id] = self.DS_list[DS_id].fnr;  
                self.client_list [self.client_id].fpr[DS_id] = self.DS_list[DS_id].fpr;  
        if (hit):   
            self.client_list[self.client_id].hit_cnt += 1
        else:               # Miss
            self.handle_miss ()

