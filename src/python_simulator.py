import numpy as np
import pandas as pd
import sys
import pickle
import random

import DataStore
import Client
import candidate
import node 
from   printf import printf
from numpy.core._multiarray_umath import dtype
#from MyConfig import MyConfing.bw_to_uInterval, settings_string 
import MyConfig 

# Codes for access algorithms
ALG_OPT             = 1 # Optimal access strategy (perfect indicator)
ALG_PGM             = 2 # PGM alg', detailed in Access Strategies journal paper
ALG_CHEAP           = 3 # Cheapest (CPI) strategy: in case of a positive indication, access the minimal-cost DS with positive indication 
ALG_ALL             = 4 # All (EPI) strategy: in case of positive indications, access all DSs with positive indications
ALG_KNAP            = 5 # Knapsack-based alg'. See Access Strategies papers.
ALG_POT             = 6 # Potential-based alg'. See Access Strategies papers.
ALG_PGM_FNO         = 7 # PGM alg', detailed in Access Strategies journal paper; Staleness Oblivious
ALG_PGM_FNA         = 8 # PGM alg', detailed in Access Strategies journal paper; staleness-aware
ALG_PGM_FNA_MR1_BY_HIST = 10 # PGM alg', detailed in Access Strategies journal paper; staleness-aware
ALG_PGM_FNA_MR1_BY_HIST_ADAPT = 11 # PGM alg', detailed in Access Strategies journal paper; staleness-aware, with adaptive alg'
ALG_OPT_HOMO        = 100 # Optimal access strategy (perfect indicator), faster version for the case of homo' accs costs

# client action: updated according to what client does
# 0: no positive ind , 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer missp, 4: no pos ind, pay missp
"""
key is an integer
"""

class Simulator(object):

    # init a list of empty DSs
    def init_DS_list(self):
        self.DS_list = [DataStore.DataStore(ID = i, size = self.DS_size, bpe = self.bpe, mr1_estimation_window = self.estimation_window, 
                        max_fpr = self.max_fpr, max_fnr = self.max_fnr, verbose = self.verbose, uInterval = self.uInterval,
                        num_of_insertions_between_estimations = self.num_of_insertions_between_estimations) 
                        for i in range(self.num_of_DSs)]
            
    def init_client_list(self):
        self.client_list = [Client.Client(ID = i, num_of_DSs = self.num_of_DSs, estimation_window = self.estimation_window, verbose = self.verbose, 
        use_redundan_coef = self.use_redundan_coef, k_loc = self.k_loc, use_adaptive_alg = self.use_adaptive_alg, missp = self.missp,
        verbose_file = self.verbose_file) 
        for i in range(self.num_of_clients)]
    
    def __init__(self, output_file, trace_file_name, alg_mode, req_df, client_DS_cost, missp, k_loc, DS_size = 1000, bpe = 15, 
                 rand_seed = 42, use_redundan_coef = False, max_fpr = 0.01, max_fnr = 0.01, verbose = 0, requested_bw = 0, 
                 uInterval = -1, use_given_loc_per_item = True):
        """
        Return a Simulator object with the following attributes:
            alg_mode:           mode of client: defined by macros above
            client_DS_cost:     2D array of costs. entry (i,j) is the cost from client i to DS j
            missp:               miss penalty
            k_loc:              number of DSs a missed key is inserted to
            DS_size:            size of DS (default 1000)
            bpe:                Bits Per Element: number of cntrs in the CBF per a cached element (commonly referred to as m/n)
            alpha:              weight for convex combination of dist-bw for calculating costs (default 0.5)
        """
        self.output_file    = output_file
        self.trace_file_name= trace_file_name
        self.missp          = missp
        self.k_loc          = k_loc
        self.DS_size        = DS_size
        self.bpe            = bpe
        self.rand_seed      = rand_seed
        self.DS_insert_mode = 1  #DS_insert_mode: mode of DS insertion (1: fix, 2: distributed, 3: ego). Currently only insert mode 1 is used

        if (self.DS_insert_mode != 1):
            print ('sorry, currently only fix insert mode (1) is supported')
            exit ()

        self.num_of_clients     = client_DS_cost.shape[0]
        self.num_of_DSs         = client_DS_cost.shape[1]
        self.client_DS_cost     = client_DS_cost # client_DS_cost(i,j) will hold the access cost for client i accessing DS j
        self.est_win_factor     = 10
        self.estimation_window  = 100 #self.DS_size / self.est_win_factor # window for parameters' estimation 
        self.max_fnr            = max_fnr
        self.max_fpr            = max_fpr
        self.verbose            = verbose # Used for debug / analysis: a higher level verbose prints more msgs to the Screen / output file.
        self.mr_of_DS           = np.zeros(self.num_of_DSs) # mr_of_DS[i] will hold the estimated miss rate of DS i 
        self.req_df             = req_df        
        self.use_redundan_coef  = use_redundan_coef
        self.use_adaptive_alg   = True if alg_mode == ALG_PGM_FNA_MR1_BY_HIST_ADAPT else False        
        self.req_cnt            = -1
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs , dtype='uint') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.leaf_of_DS         = np.array(np.floor(np.log2(self.client_DS_cost))).astype('uint8') # lg_client_DS_cost(i,j) will hold the lg2 of access cost for client i accessing DS j
        self.pos_ind_list    = [] #np.array (0, dtype = 'uint8') #list of the DSs with pos' ind' (positive indication) for the current request
        self.q_estimation       = np.zeros (self.num_of_DSs , dtype='uint') #q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.window_alhpa       = 0.25 # window's alpha parameter for estimated parameters       
        
        if (alg_mode == ALG_PGM_FNA or alg_mode == ALG_PGM_FNA_MR1_BY_HIST_ADAPT):
            print ('You chose an obsolete alg mode. Please verify the relevant code.')
            exit ()
        self.alg_mode           = alg_mode

        # Statistical parameters (collected / estimated at run time)
        self.total_cost         = float(0)
        self.hit_cnt            = 0
        self.total_access_cost  = 0
        self.high_cost_mp_cnt   = 0 # counts the misses for cases where accessing DSs was too costly, so the alg' decided to access directly the mem
        self.comp_miss_cnt      = 0
        self.non_comp_miss_cnt  = 0
        self.FN_miss_cnt        = 0 # num of misses happened due to FN event
        self.tot_num_of_updates = 0
        self.requested_bw       = requested_bw
        
        # If the uInterval is given in the input (as a non-negative value) - use it. 
        # Else, calculate uInterval by the given requested_bw parameter.
        if (uInterval == -1): # Should calculate uInterval by the given requested_bw parameter
            self.use_global_uInerval = True
            self.uInterval = MyConfig.bw_to_uInterval (self.DS_size, self.bpe, self.num_of_DSs, requested_bw)
            self.update_cycle_of_DS = np.array ( [ds_id * self.uInterval / self.num_of_DSs for ds_id in range (self.num_of_DSs)]) 
        else:
            self.use_global_uInerval = False
            self.uInterval = uInterval
        self.cur_updating_DS = 0
        #self.num_of_insertions_between_estimations_factor = 100
        self.use_only_updated_ind = True if (uInterval == 1) else False
        self.num_of_insertions_between_estimations = np.uint8 (50)
        self.use_given_loc_per_item = use_given_loc_per_item # When True, upon miss, the missed item is inserted to the location(s) specified in the given request traces input. When False, it's randomized for each miss request.

        self.avg_DS_accessed_per_req = float(0)
        if (self.alg_mode == ALG_PGM_FNA_MR1_BY_HIST):
            self.verbose_file = open ("../res/debug.txt", "w", buffering=1)
        elif (self.alg_mode == ALG_PGM_FNO):
            self.verbose_file = open ("../res/fno.txt", "w", buffering=1)

        self.init_DS_list() #DS_list is the list of DSs
        self.init_client_list ()
            


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
        self.mean_service_cost  = self.total_cost / self.req_cnt 
        self.settings_str       = MyConfig.settings_string (self.trace_file_name, self.DS_size, self.bpe, self.req_cnt, self.num_of_DSs, self.k_loc, self.missp, self.requested_bw, self.uInterval, self.alg_mode)
        printf (self.output_file, '\n\n{} | service_cost = {}\n'  .format (self.settings_str, self.mean_service_cost))
        bw_in_practice =  int (round ( self.tot_num_of_updates * self.DS_size * self.bpe * (self.num_of_DSs - 1) / self.req_cnt) ) #Each update is a full indicator, sent to n-1 DSs)
        if (self.requested_bw != bw_in_practice):
            printf (self. output_file, '//Note: requested bw was {:.0f}, but actual bw was {:.0f}\n' .format (self.requested_bw, bw_in_practice))
        printf (self.output_file, '// tot_access_cost= {}, hit_ratio = {:.2}, non_comp_miss_cnt = {}, comp_miss_cnt = {}\n' .format 
           (self.total_access_cost, self.hit_ratio, self.non_comp_miss_cnt, self.comp_miss_cnt) )                                 
        num_of_fpr_fnr_updates = sum (DS.num_of_fpr_fnr_updates for DS in self.DS_list) / self.num_of_DSs
        printf (self.output_file, '// estimation window = {}, ' .format (self.estimation_window))
        if (self.alg_mode == ALG_PGM_FNA_MR1_BY_HIST):
            printf (self.output_file, '// num of insertions between fpr_fnr estimations = {}\n' .format (self.num_of_insertions_between_estimations))
            printf (self.output_file, '// avg num of fpr_fnr_updates = {:.0f}, fpr_fnr_updates bw = {:.4f}\n' 
                                .format (num_of_fpr_fnr_updates, num_of_fpr_fnr_updates/self.req_cnt))
        

    def run_trace_opt_hetro (self):
        """
        Run a full trace as Opt access strat' when the DS costs are heterogneous
        """
        for self.req_cnt in range(self.req_df.shape[0]): # for each request in the trace... 
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.calc_client_id()
            # get the list of caches holding the request
            true_answer_DS_list = np.array([DS_id for DS_id in range(self.num_of_DSs) if (self.cur_req.key in self.DS_list[DS_id])])

            if true_answer_DS_list.size == 0: # Miss: request is indeed not found in any DS 
                self.client_list[self.client_id].comp_miss_cnt += 1
                self.insert_key_to_DSs_without_indicator () # Opt doesn't really use indicators - it "knows" the actual contents of the DSs
            else:  # hit
                # find the cheapest DS holding the request
                access_DS_id = true_answer_DS_list[np.argmin( np.take( self.client_DS_cost[self.client_id] , true_answer_DS_list ) )]
                # We assume here that the cost of every DS < missp
                # update variables
                self.client_list[self.client_id].total_access_cost += self.client_DS_cost[self.client_id][access_DS_id]
                if (self.verbose == 3):
                    self.client_list[self.client_id].add_DS_accessed(self.req_cnt, [access_DS_id])
                # perform access. we know it will be successful
                self.DS_list[access_DS_id].access(self.cur_req.key)
                self.client_list[self.client_id].hit_cnt += 1

    def consider_send_update (self):
        if (not(self.use_global_uInerval)): # To be used only if we have a "globally calculated uInterval"
            return
        remainder = self.req_cnt % self.uInterval
        for ds_id in range (self.num_of_DSs):
            if (remainder == self.update_cycle_of_DS[ds_id]):
                check_delta_th = False #True if (self.req_cnt > (self.num_of_DSs * self.DS_size)) else False
                self.DS_list[ds_id].send_update (check_delta_th)
                self.tot_num_of_updates += 1


    def run_trace_pgm_fno_hetro (self):
        """
        Run a full trace where the access strat' is the PGM, as proposed in the journal paper "Access Srategies for Network Caching".
        """
        for self.req_cnt in range(self.req_df.shape[0]): # for each request in the trace... 
            self.consider_send_update ()
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.calc_client_id()
            
            # self.pos_ind_list <- list of DSs with positive indications
            self.pos_ind_list = np.array ([int(DS.ID) for DS in self.DS_list if (self.cur_req.key in DS.updated_indicator) ]) if self.use_only_updated_ind else \
                                np.array ([int(DS.ID) for DS in self.DS_list if (self.cur_req.key in DS.stale_indicator) ]) 
            if (len(self.pos_ind_list) == 0): # No positive indications --> FNO alg' has a miss
                self.handle_miss (consider_fpr_fnr_update = False)
                continue        
            self.estimate_mr_by_history () # Update the estimated miss rates of the DSs; the updated miss rates of DS i will be written to mr_of_DS[i]   
            self.access_pgm_fno_hetro ()

    def run_trace_pgm_fna_hetro (self):
        """
        Run a full trace where the access strat' is PGM, at its "Staleness-Aware" version. 
        The simplified, False-Negative-Obvliious alg' of PGM, is detailed in the journal paper "Access srategies for Network Caching".
        """
        self.PGM_FNA_partition ()
            
        for self.req_cnt in range(self.req_df.shape[0]): # for each request in the trace... 
            self.consider_send_update ()
            self.cur_req = self.req_df.iloc[self.req_cnt]  
            self.calc_client_id ()
            for i in range (self.num_of_DSs):
                self.indications[i] = True if (self.cur_req.key in self.DS_list[i].stale_indicator) else False #self.indication[i] holds the indication of DS i for the cur request
            verbose         = 4 if (self.verbose == 4 and self.req_cnt > 20000) else 0  
            self.mr_of_DS   = self.client_list [self.client_id].get_mr_given_mr1 (self.indications, np.array([DS.mr0_cur for DS in self.DS_list]), np.array([DS.mr1_cur for DS in self.DS_list]), verbose) 
            self.access_pgm_fna_hetro ()

    def calc_client_id (self):
        self.client_id = self.cur_req.client_id if (self.use_given_loc_per_item) else random.randint(0, self.num_of_DSs-1)



    def run_simulator (self):
        """
        Run a simulation, gather statistics and prints outputs
        """
        np.random.seed(self.rand_seed)
        num_of_req = self.req_df.shape[0]
        if self.alg_mode == ALG_OPT:
            self.run_trace_opt_hetro ()
            self.gather_statistics ()
        elif self.alg_mode == ALG_PGM_FNO:
            self.run_trace_pgm_fno_hetro ()
            self.gather_statistics ()
        elif (self.alg_mode == ALG_PGM_FNA or self.alg_mode == ALG_PGM_FNA_MR1_BY_HIST or self.alg_mode == ALG_PGM_FNA_MR1_BY_HIST_ADAPT):
            self.speculate_accs_cost    = 0 # Total accs cost paid for speculative accs
            self.speculate_accs_cnt     = 0 # num of speculative accss, that is, accesses to a DS despite a miss indication
            self.speculate_hit_cnt      = 0 # num of hits among speculative accss
            self.indications            = np.array (range (self.num_of_DSs), dtype = 'bool')
            self.run_trace_pgm_fna_hetro ()
            self.gather_statistics()
#             avg_num_of_updates_per_DS = sum (DS.num_of_updates for DS in self.DS_list) / self.num_of_DSs
#             avg_update_interval = -1 if (avg_num_of_updates_per_DS == 0) else self.req_cnt / avg_num_of_updates_per_DS
            if (self.verbose == 1):
                printf (self.output_file, '// spec accs cost = {:.0f}, num of spec hits = {:.0f}' .format (self.speculate_accs_cost, self.speculate_hit_cnt))             
        else: 
            printf (self.output_file, 'Wrong alg_mode: {:.0f}\n' .format (self.alg_mode))

        
    def estimate_mr_by_history (self):
        """
        Update the estimated miss rate of each DS, based on the history.
        This estimation is good only for non-speculative accesses, i.e. accesses after a positive ind' (mr[1]) 
        """
        self.mr_of_DS = np.array([DS.mr1_cur for DS in self.DS_list]) # For each 1 <= i<= n, Copy the miss rate estimation of DS i to mr_of_DS(i)

    def handle_compulsory_miss (self, consider_fpr_fnr_update = True):
        """
        Called upon a compulsory miss, namely, a fail to retreive a request from any DS, while the request is indeed not stored in any of the DSs.
        The func' increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        self.client_list[self.client_id].comp_miss_cnt += 1
        self.insert_key_to_DSs (consider_fpr_fnr_update = consider_fpr_fnr_update)

    def handle_non_compulsory_miss (self, consider_fpr_fnr_update = True):
        """
        Called upon a non-compulsory miss, namely, a fail to retreive a request from any DS, while the request is actually stored in at least one DS.
        The func' increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        self.client_list[self.client_id].non_comp_miss_cnt += 1
        self.insert_key_to_DSs (consider_fpr_fnr_update = consider_fpr_fnr_update)
        if (self.alg_mode == ALG_PGM_FNO):
            self.FN_miss_cnt += 1

    def handle_miss (self, consider_fpr_fnr_update = True):
        """
        Called upon a miss. Check whether the miss is compulsory or not. Increments the relevant counter, and inserts the key to self.k_loc DSs.
        """
        if (self.is_compulsory_miss()):
            self.handle_compulsory_miss (consider_fpr_fnr_update = consider_fpr_fnr_update)
        else:
            self.handle_non_compulsory_miss (consider_fpr_fnr_update = consider_fpr_fnr_update)

    def insert_key_to_closest_DS(self, req):
        """
        Currently unused
        """
        # check to see if one needs to insert key to closest cache too
        if self.DS_insert_mode == 2:
            self.DS_list[self.client_id].insert(req.key)

    def insert_key_to_DSs_without_indicator (self):
        """
        insert key to all k_loc DSs.
        The DSs to which the key is inserted are either: 
        - Defined by the input (parsed) trace (if self.use_given_loc_per_item==True)
        - Chosen as a "hash" (actually, merely a modulo calculation) of the key 
        Do not use indicator. Used for Opt, which doesn't need indicators.
        """
        if (self.use_given_loc_per_item):
            for i in range(self.k_loc):
                self.DS_list[self.cur_req['%d'%i]].insert (key = self.cur_req.key, use_indicator = False)
        else:
            for i in range(self.k_loc):
                self.DS_list[(self.cur_req.key+i) % self.num_of_DSs].insert (key = self.cur_req.key, use_indicator = False)
            
    def insert_key_to_DSs(self, consider_fpr_fnr_update = True):
        """
        insert key to all k_loc DSs.
        The DSs to which the key is inserted are either: 
        - Defined by the input (parsed) trace (if self.use_given_loc_per_item==True)
        - Chosen as a "hash" (actually, merely a modulo calculation) of the key 
        """
        if (self.use_given_loc_per_item):
            for i in range(self.k_loc):
                self.DS_list[self.cur_req['%d'%i]].insert (key = self.cur_req.key, req_cnt = self.req_cnt, consider_fpr_fnr_update = consider_fpr_fnr_update)
        else:
            for i in range(self.k_loc):
                self.DS_list[(self.cur_req.key+i) % self.num_of_DSs].insert (key = self.cur_req.key, req_cnt = self.req_cnt, consider_fpr_fnr_update = consider_fpr_fnr_update)
        
        
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
        #self.pos_ind_list = [int(i) for i in self.pos_ind_list] # cast pos_ind_list to int

        # Partition stage
        ###############################################################################################################
        # leaf_of_DS (i,j) holds the leaf to which DS with cost (i,j) belongs, that is, log_2 (DS(i,j))

        # leaves_of_DSs_w_pos_ind will hold the leaves of the DSs with pos' ind'
        cur_num_of_leaves = np.max (np.take(self.leaf_of_DS[self.client_id], self.pos_ind_list)) + 1

        # DSs_in_leaf[j] will hold the list of DSs which belong leaf j, that is, the IDs of all the DSs with access in [2^j, 2^{j+1})
        DSs_in_leaf = [[]]
        for leaf_num in range (cur_num_of_leaves):
            DSs_in_leaf.append ([])
        for ds in (self.pos_ind_list):
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
            self.handle_miss (consider_fpr_fnr_update = False)
            return

        # Now we know that the alg' decided to access at least one DS
        # Add the costs and IDs of the selected DSs to the statistics
        self.client_list[self.client_id].total_access_cost += final_sol.ac
        if (self.verbose == 3):
            self.client_list[self.client_id].add_DS_accessed(self.cur_req.req_id, final_sol.DSs_IDs)

        # perform access. the function access() returns True if successful, and False otherwise
        accesses = np.array([self.DS_list[DS_id].access(self.cur_req.key) for DS_id in final_sol.DSs_IDs])
        if any(accesses):   #hit
            self.client_list[self.client_id].hit_cnt += 1
        else:               # Miss
            self.handle_miss (consider_fpr_fnr_update = False)


    def access_pgm_fna_hetro (self):

        req                     = self.cur_req

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

        # perform access
        self.sol = final_sol.DSs_IDs
        hit = False
        for DS_id in final_sol.DSs_IDs:
            is_speculative_accs = not (self.indications[DS_id])
            if (is_speculative_accs): #A speculative accs 
                self.                             speculate_accs_cost += self.client_DS_cost [self.client_id][DS_id] # Update the whole system's data (used for statistics)
                self.client_list [self.client_id].speculate_accs_cost += self.client_DS_cost [self.client_id][DS_id] # Update the relevant client's data (used for adaptive / learning alg') 
            if (self.DS_list[DS_id].access(self.cur_req.key, is_speculative_accs)): # hit
                if (not (hit) and (not (self.indications[DS_id]))): # this is the first hit; for each speculative req, we want to count at most a single hit 
                    self.                             speculate_hit_cnt += 1  # Update the whole system's speculative hit cnt (used for statistics) 
                    self.client_list [self.client_id].speculate_hit_cnt += 1  # Update the relevant client's speculative hit cnt (used for adaptive / learning alg')
                hit = True
                self.client_list [self.client_id].fnr[DS_id] = self.DS_list[DS_id].fnr;  
                self.client_list [self.client_id].fpr[DS_id] = self.DS_list[DS_id].fpr;  
        if (hit):   
            self.client_list[self.client_id].hit_cnt += 1
        else: # Miss
            self.handle_miss ()


