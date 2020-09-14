import numpy as np
import pandas as pd
import DataStore
import Client
import candidate
import node
import sys
import pickle
import sys

# Codes for access algorithms
ALG_OPT     = 1 # Optimal access strategy (perfect indicator)
ALG_PGM     = 2 # PGM alg', detailed in Access Strategies journal paper
ALG_CHEAP   = 3 # Cheapest (CPI) strategy: in case of a positive indication, access the minimal-cost DS with positive indication 
ALG_ALL     = 4 # All (EPI) strategy: in case of positive indications, access all DSs with positive indications
ALG_KNAP    = 5 # Knapsack-based alg'. See Access Strategies papers.
ALG_POT     = 6 # Potential-based alg'. See Access Strategies papers.
ALG_PGM_FNO = 7 # PGM alg', detailed in Access Strategies journal paper; False-Negative Oblivious
ALG_PGM_FNA = 8 # PGM alg', detailed in Access Strategies journal paper; False-Negative Aware
NUM_OF_ALGS = 8 # Number of algs'

# client action: updated according to what client does
# 0: no positive ind , 1: hit upon access of DSs, 2: miss upon access of DSs, 3: high DSs cost, prefer missp, 4: no pos ind, pay missp
"""
key is an integer
"""

class Simulator(object):

    # init a list of empty DSs
    def init_DS_list(self):
        return [DataStore.DataStore(ID = i, size = self.DS_size, bpe = self.bpe, estimation_window = self.estimation_window) for i in range(self.num_of_DSs)]
            
    def init_client_list(self):
        return [Client.Client(ID = i, num_of_DSs = self.num_of_DSs, estimation_window = self.estimation_window, verbose = self.verbose) for i in range(self.num_of_clients)]
    
    def __init__(self, alg_mode, DS_insert_mode, req_df, client_DS_cost, missp, k_loc, DS_size = 1000, bpe = 15, rand_seed = 42, verbose = 0, uInterval = 1):
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
            uInterval:          Update interval - number of requests to each DS between updates it sends. Updates are assumed to arrive immediately. Currently unused
        """
        #self.uInterval      = uInterval # update interval
        self.alg_mode       = alg_mode
        self.missp          = missp
        self.k_loc          = k_loc
        self.DS_size        = DS_size
        self.bpe            = bpe
        self.rand_seed      = rand_seed
        self.DS_insert_mode = DS_insert_mode
        # self.always_updated_indicator = True if (self.uInterval == 1) else False

        self.num_of_clients     = client_DS_cost.shape[0]
        self.num_of_DSs         = client_DS_cost.shape[1]
        self.client_DS_cost     = client_DS_cost # client_DS_cost(i,j) will hold the access cost for client i accessing DS j
        self.estimation_window  = self.DS_size # window for parameters' estimation 
        self.verbose            = verbose # Used for debug / analysis: a higher level verbose prints more msgs to the Screen / output file.
        self.DS_list            = self.init_DS_list() #DS_list is the list of DSs
        self.mr_of_DS           = np.zeros(self.num_of_DSs) # mr_of_DS[i] will hold the estimated miss rate of DS i 
        self.req_df             = req_df        
        self.client_list        = self.init_client_list ()
        self.req_cnt        = float(-1)
        self.pos_ind_cnt        = np.zeros (self.num_of_DSs , dtype='uint') #pos_ind_cnt[i] will hold the number of positive indications of indicator i in the current window
        self.lg_client_DS_cost  = np.array(np.floor(np.log2(self.client_DS_cost))).astype('uint8') # lg_client_DS_cost(i,j) will hold the lg2 of access cost for client i accessing DS j
        self.cur_pos_DS_list    = [] #np.array (0, dtype = 'uint8') #list of the DSs with pos' ind' (positive indication) for the current request
        self.q_estimation       = np.zeros (self.num_of_DSs , dtype='uint') #q_estimation[i] will hold the estimation for the prob' that DS[i] gives positive ind' for a requested item.  
        self.window_alhpa       = 0.25 # window's alpha parameter for estimated parameters 

        # Statistical parameters (collected / estimated at run time)
        self.total_cost         = float(0)
        self.access_cnt         = float(0)
        self.hit_cnt            = float(0)
        self.comp_miss_cnt      = float(0)
        self.avg_DS_hit_ratio   = float(0)
        self.total_access_cost  = float(0)
        self.high_cost_mp_cnt   = float(0) # counts the misses for cases where accessing DSs was too costly, so the alg' decided to access directly the mem
        self.non_comp_miss_cnt  = float(0)

        # Debug / verbose variables
        if (self.verbose == 1):
            self.num_DS_accessed = float(0) #Currently unused
            self.avg_DS_accessed_per_req = float(0)

    # Returns an np.array of the DSs with positive ind'
    def get_indications(self):
        self.cur_pos_DS_list = np.array([DS.ID for DS in self.DS_list if (DS.get_indication(self.cur_req.key))])

    # Returns true iff key is found in at least of one of DSs specified by DS_index_list
    def req_in_DS_list(self, key, DS_index_list):
        for i in DS_index_list:
            if (key in self.DS_list[i]):
                return True
        return False

    def PGM_FNA_partition (self):
        """
        Performs the partition stage in the PGM-False-Negative-Aware alg'. 
        In a FNA (aka stale-aware) alg', the candidate DSs are all the DSs (even those with negative ind'), and
        therefore it's possible to perform the partition stage only once, instead of re-run it for each request.
        """
        self.DSs_in_leaf = [] # DSs_in_leaf[i] will hold the 2D list of DSs of client i, as described below
        self.num_of_leaves = np.zeros (self.num_of_clients, dtype = 'uint8') #num_of_leaves[i] will hold the # of leaves in PGM alg' when the client is i
        for client_id in range (self.num_of_clients):
            self.num_of_leaves[client_id] = np.max (np.take(self.lg_client_DS_cost [client_id], range (self.num_of_DSs))) + 1 
            DSs_in_leaf = [[]] # For the current client_id, DSs_in_leaf[j] will hold the list of DSs which belong leaf j, that is, the IDs of all the DSs with access in [2^j, 2^{j+1})
            for leaf_num in range (self.num_of_leaves[client_id]):
                DSs_in_leaf.append ([])
            for ds_id in (range(self.num_of_DSs)):
                DSs_in_leaf[self.lg_client_DS_cost[client_id][ds_id]].append(ds_id)
            self.DSs_in_leaf.append(DSs_in_leaf)



    def gather_statistics(self):
        """
        Accumulates and organizes the stat collected during the sim' run.
        This func' is usually called once at the end of each run of the python_simulator.
        """
        self.total_access_cost  = np.sum( [client.total_access_cost for client in self.client_list ] ) 
        self.access_cnt         = np.sum( [client.access_cnt for client in self.client_list ] )
        self.hit_cnt            = np.sum( [client.hit_cnt for client in self.client_list ] )
        if (self.access_cnt == 0):
            print ('warning: total number of DS accesses is 0')
            self.hit_ratio               = 0
            self.avg_DS_accessed_per_req = 0
        else:
            self.hit_ratio               = float(self.hit_cnt) / self.req_cnt
            if (self.verbose == 1):
                self.num_DS_accessed     = np.sum( [sum(client.num_DS_accessed) for client in self.client_list ] )
                self.avg_DS_accessed_per_req = float(self.num_DS_accessed) / self.access_cnt
        self.non_comp_miss_cnt  = np.sum( [client.non_comp_miss_cnt for client in self.client_list ] )
        self.comp_miss_cnt      = np.sum( [client.comp_miss_cnt for client in self.client_list ] )
        self.high_cost_mp_cnt   = np.sum( [client.high_cost_mp_cnt for client in self.client_list ] )
        self.total_cost         = self.total_access_cost + self.missp * (self.comp_miss_cnt + self.non_comp_miss_cnt + self.high_cost_mp_cnt)
        self.avg_DS_hit_ratio   = np.average ([DS.get_hr() for DS in self.DS_list])
    
    def start_simulator(self):
        # print ('alg_mode=%d, kloc = %d, missp = %d, insertion_mode=%d' % (self.alg_mode, self.k_loc, self.missp, self.DS_insert_mode))

        if (self.alg_mode == ALG_PGM_FNA):
            # For the FNA, all the DSs may be accessed. Hence, the partition stage can be performed only once, for all the DSs, regardless the indications. 
            self.PGM_FNA_partition () # perform the partition stage for all clients, based on the weights, that are already known.
            if (self.verbose == 2):
                self.debug_file = open ("../res/fna.txt", "w")

        np.random.seed(self.rand_seed)
        for req_id in range(self.req_df.shape[0]): # for each request in the trace... 
            if self.DS_insert_mode == 3: # ego mode: n/(n+1) of the requests enter to a random DS. 1/(n+1) of the requests belong to a single "ego" client.
                # re-assign the request to client 0 w.p. 1/(self.num_of_clients+1) and handle it. otherwise, put it in a random DS
                if np.random.rand() < 1/(self.num_of_clients+1):
                    self.req_df.set_value(req_id, 'client_id', 0)
                    self.handle_request(self.req_df.iloc[req_id])
                else:
                    self.insert_key_to_random_DSs(self.req_df.iloc[req_id]) # NOTE: the current imp' of insert_key_to_random_DSs() isn't really random... 
            else: # fix or distributed mode
                self.handle_request(self.req_df.iloc[req_id]) #fix mode (the one that is currently used): assign each request to a single DS, which is picked uar from all DSs  
                if self.DS_insert_mode == 2: # distributed mode
                    self.insert_key_to_closest_DS(self.req_df.iloc[req_id])
        self.gather_statistics()
        print ('alg_mode = %d, tot_cost=%.2f, tot_access_cost= %.2f, hit_ratio = %.2f, non_comp_miss_cnt = %d, comp_miss_cnt = %d, access_cnt = %d' % 
                 (self.alg_mode, self.total_cost, self.total_access_cost, self.hit_ratio, self.non_comp_miss_cnt, self.comp_miss_cnt, self.access_cnt)        )
        
    def update_mr_of_DS (self):
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
            self.DS_list[self.cur_req['%d'%i]].insert (self.cur_req.key, False) 
            
 
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

    def send_update (self):
        """
        Force all the DSs to send an updated indicator, summarizing their fresh content.
        """
        if (DS in self.DS_list):
            DS.send_update ()

    def handle_request(self, req):
        """
        Handle a user req:
        - Extract the req id, req cnt and client_id.
        - Get the indications from all the indicators.
        - Run a concrete access strategy (\cpi, \epi, PGM_FNA etc.) 

        """
        self.cur_req = req
        self.req_cnt += 1
        self.client_id = self.cur_req.client_id

        if self.alg_mode == ALG_OPT:
            self.access_opt ()
            return
        self.get_indications() # self.cur_pos_DS_list <- list of DSs with positive indications
        if self.alg_mode == ALG_PGM_FNO:
            self.update_mr_of_DS() # Update the estimated miss rates of the DSs; the updated miss rates of DS i will be written to mr_of_DS[i]   
            self.access_pgm_fno ()
        elif self.alg_mode == ALG_PGM_FNA:
            self.access_pgm_fna ()
        else: 
            print ('Wrong alg_code')

    def access_opt (self):
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

        # return
                
    def access_pgm_fno (self):
        """
        The PGM FNO (false negative oblivious) alg' detailed in the paper: Access Strategies for Network Caching, Journal verison.
        """ 
        if (len(self.cur_pos_DS_list) == 0): # No positive indications --> FNO alg' has a miss
            if (self.verbose == 2):
                print ('req cnt = ', self.req_cnt, 'pos ind = {}, mr = ', self.mr_of_DS)
            self.handle_miss ()
            return
        
        # Now we know that there exists at least one positive indication
        req = self.cur_req
        self.cur_pos_DS_list = [int(i) for i in self.cur_pos_DS_list] # cast cur_pos_DS_list to int

        # Partition stage
        ###############################################################################################################
        # leaf_of_DS (i,j) will hold the leaf to which DS with cost (i,j) belongs, that is, log_2 (DS(i,j))
        self.leaf_of_DS = np.array(np.floor(np.log2(self.client_DS_cost)))
        self.leaf_of_DS = self.leaf_of_DS.astype('uint8')

        # leaves_of_DSs_w_pos_ind will hold the leaves of the DSs with pos' ind'
        cur_num_of_leaves = np.max (np.take(self.leaf_of_DS[self.client_id], self.cur_pos_DS_list)) + 1

        # DSs_in_leaf[j] will hold the list of DSs which belong leaf j, that is, the IDs of all the DSs with access in [2^j, 2^{j+1})
        DSs_in_leaf = [[]]
        for leaf_num in range (cur_num_of_leaves):
            DSs_in_leaf.append ([])
        for ds in (self.cur_pos_DS_list):
            DSs_in_leaf[self.lg_client_DS_cost[self.client_id][ds]].append(ds)

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
            self.client_list[self.client_id].add_DS_accessed(req.req_id, final_sol.DSs_IDs)
        self.client_list[self.client_id].access_cnt += 1

        # perform access. the function access() returns True if successful, and False otherwise
        if (self.verbose == 2):
            print ('req cnt = ', self.req_cnt, 'pos ind = ', self.cur_pos_DS_list, 'mr = ', self.mr_of_DS, 'accss = ', final_sol.DSs_IDs)
        accesses = np.array([self.DS_list[DS_id].access(req.key) for DS_id in final_sol.DSs_IDs])
        if any(accesses):   #hit
            self.client_list[self.client_id].hit_cnt += 1
        else:               # Miss
            self.handle_miss ()
        return


    def access_pgm_fna (self):

        req                     = self.cur_req
        self.mr_of_DS           = self.client_list [self.client_id].get_mr (self.cur_pos_DS_list) # Get the probability that the requested item is in DS i, given to the concrete indication of its indicator
        self.cur_pos_DS_list    = [int(i) for i in self.cur_pos_DS_list] # cast cur_pos_DS_list to int

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
            if (self.verbose == 2):
                #self.debug_file.write ('req cnt = %d' .format (self.req_cnt)) #, 'pos ind = ', self.cur_pos_DS_list, 'mr = ', self.mr_of_DS, 'accss = ', final_sol.DSs_IDs)
                print ('req cnt = ', self.req_cnt, 'pos ind = ', self.cur_pos_DS_list, 'mr = ', self.mr_of_DS, 'accss = ', final_sol.DSs_IDs)
            self.handle_miss ()
            return

        # Now we know that the alg' decided to access at least one DS
        # Add the costs and IDs of the selected DSs to the statistics
        if (self.verbose == 2):
            print ('req cnt = ', self.req_cnt, 'pos ind = ', self.cur_pos_DS_list, 'mr = ', self.mr_of_DS, 'accss = ', final_sol.DSs_IDs)
        self.client_list[self.client_id].total_access_cost += final_sol.ac
        if (self.verbose == 1):
            self.client_list[self.client_id].add_DS_accessed(req.req_id, final_sol.DSs_IDs)
        self.client_list[self.client_id].access_cnt += 1

        # perform access. the function DataStore.access() returns True iff the access is a hit
        hit = False
        for DS_id in final_sol.DSs_IDs:
            if (self.DS_list[DS_id].access(req.key)): # hit
                hit = True
                # print ("fnr = %.2f, fpr = %.4f" %(self.DS_list[DS_id].fnr_fpr[0], self.DS_list[DS_id].fnr_fpr[1]))
                self.client_list[self.client_id].update_fnr_fpr (self.DS_list[DS_id].fnr_fpr, DS_id) # each hit DS piggybacks to the client the updated estimated fpr, fnr; the client uses this info to update its estimation for the mr0, mr1 of this DS 
        if (hit):   
            self.client_list[self.client_id].hit_cnt += 1
        else:               # Miss
            self.handle_miss ()
        return
