## Access Strategies for Network Caching with Staleness

This project provides tools to simulate several access strategies for distributed caching. 
The simulator considers a user equipped with several caches, that receives periodical updates about the cached content. These updates are not totally accurate. In particular, there exist not only false-positives, but also false-negatives, due to the staleness of the indicator that the user has. 
The user has to select which caches to access, to obtain the requested datum at the lowest price and maximum certainty possible.
For details about the problem and the algorithms used, please refer to the papers:

[1] I. Cohen, Gil Einziger, R. Friedman, and G. Scalosub, [Access Strategies for Network Caching](https://www.researchgate.net/profile/Itamar-Cohen-2/publication/346732877_Access_Strategies_for_Network_Caching/links/5fd27eeea6fdcc697bf6f924/Access-Strategies-for-Network-Caching.pdf), IEEE Transactions on Networking, Vol. 29(2), 2021, pp.609-622.
 
[2] I. Cohen, Gil Einziger, and G. Scalosub, [False Negative Awareness in Indicator-based Caching Systems](https://www.researchgate.net/publication/361178366_False_Negative_Awareness_in_Indicator-Based_Caching_Systems), IEEE Transactions on Networking, 2022, pp. 46-56.

The source files are described below. More detailed documentation is found within the source files.

# Directories
All source files are written in Python, and found in ./src.

The result files are written to ./res.

# source files

##### runner.py #
Runs a simulation, looping over all requested values of parameters (miss penalty, cache sizes, number of caches etc.).

##### python_simulator.py #
Implements the class Simulator = a simulator that accepts system parameters (trace, number and size of caches, algorithm to run etc.), runs a simulation, and outputs the results to a file.

##### client.py
Implements the client-side algorithm (CS_{FNA}), described in the paper [2].

In addition, the client may be run in a degenerated mode, for simulating other, benchmark, access strategies.

##### DataStore.py
The class for a DataStore (cache). The cache stores items using the LRU policy.
It also implements the cache-side algorithm for estimating FPR (false-positive ratio) and FNR (false-negative ratio), as described in the paper [2].
The cache itself is implemented in the file mod_pylru.py.

###### mod_pylru.py
Implementation of an LRU cache. Source code is taken from:
Copyright (C) Jay Hutchinson
https://github.com/jlhutch/pylru

##### SimpleBloomFilter.py, CountingBloomFilter.py, 
A Simple and a Counting Bloom filter. To study more about Bloom filters, consider the following links: [1](http://www.maxburstein.com/blog/creating-a-simple-bloom-filter), [2](https://hur.st/bloomfilter), [3](http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html), [4](https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf).

##### Wiki_parser.py
Parses a WikiBench trace, 
Output: a csv file, where:
        - the first col. is the keys,
        - the 2nd col. is the id of the clients of this req,
        - the rest of the cols. are the locations ("k_loc") to which a central controller would enter this req. upon a miss. 

##### Keys_list_parser.py
Parses a trace whose format is merely a list of keys (each key in a different line). 
Output: a csv file, where:
        - the first col. is the keys,
        - the 2nd col. is the id of the clients of this req,
        - the rest of the cols. are the locations ("k_loc") to which a central controller would enter this req. upon a miss.


###### gen_graph.py
generates the OVH network which exemplifies a commercial CDN (Content Delivery Network), and saves it on a .csv file.

###### gen_cost_hist.py
Generates a histogram showing the cache access costs for the OVH network

##### MyConfig.py
This file contains several accessory functions, used throughout the project, e.g. for generating the string describing the settings of the simulation, and generate the list of requests from the trace.

##### node.py
An accessory function for the PGM access strategy algorithm, described in the paper [1]. 
The function merges 2 input nodes.
Inputs: 2 nodes to merge, H and L, and r = log(beta)
Ouput: res_node - the result node, res_node.

##### candidate.py
A candidate sol' for the DSS (Data-Store-Selection) prob' (aka the "Cache-Selection" problem), consists of a list of DSs, theirs aggregate (mult' of) miss rate, and aggregate (sum of) accs cost

##### printf.py
An accessory function for format-printing to a file.

##### check_fpr_fnr_formulas.py
A script to compare the fpr, fnr, calculated by either the paper [2], or by an an older paper, which uses different model:
[3] Y. Zhu and H. Jiang, [False rate analysis of bloom filter replicas in distributed systems](https://ieeexplore.ieee.org/iel5/11126/35641/01690627.pdf?casa_token=pSq-4ffVpNQAAAAA:olg2HqQzUd4-tj054HUiYce2xRoxswYKlEFnOnSmKy_Sta5-7KLWjPRjZUFwip9B5ez1qlJHSA), ICPP, 2006, pp. 255-262.

