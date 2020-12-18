from printf import printf 

trace_idx       = 0
cache_size_idx  = 1
bpe_idx         = 2
num_of_req_idx  = 3
num_of_DSs_idx  = 4
kloc_idx        = 5
missp_idx       = 6
bw_idx          = 7
uInterval_idx   = 8
alg_idx         = 9
num_of_fields   = alg_idx + 1


class Res_file_parser (object):  

    def __init__ (self):
        """
        """
        self.add_plot_opt   = '\t\t\\addplot [color = green, mark=+, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.add_plot_str1  = '\t\t\\addplot [color = blue, mark=square, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.add_plot_fno1  = '\t\t\\addplot [color = purple, mark=o, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.add_plot_fna1  = '\t\t\\addplot [color = red, mark=triangle*, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.add_plot_fno2  = '\t\t\\addplot [color = cyan, mark=x, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.add_plot_fna2  = '\t\t\\addplot [color = black, mark=triangle, line width = \\plotLineWidth] coordinates {\n\t\t'
        self.end_add_plot_str = '\n\t\t};'
        self.add_legend_str = '\n\t\t\\addlegendentry {'
        self.add_plot_str_dict = {'FNA' : self.add_plot_fna2, 'FNO' : self.add_plot_fno2}
        self.legend_entry_dict = {'FNA' : '\\pgmfna', 'FNO' : '\\pgmfno'}

    def parse_line (self, line):
        splitted_line = line.split ("|")
         
        settings        = splitted_line[0]
        cost            = float(splitted_line[1].split(" = ")[1])
        splitted_line   = settings.split (".")

        if len (splitted_line) < num_of_fields:
            print ("encountered a format error")
            return False
        self.dict = {
            "trace"      : splitted_line        [trace_idx],
            "cache_size" : int (splitted_line   [cache_size_idx].split("C")[1].split("K")[0]),   
            "bpe"        : int (splitted_line   [bpe_idx]       .split("bpe")[1]),
            "num_of_req" : splitted_line        [num_of_req_idx].split("req")[0],
            "num_of_DSs" : int (splitted_line   [num_of_DSs_idx].split("DSs")[0]), 
            "Kloc"       : int (splitted_line   [kloc_idx]      .split("Kloc")[1]),
            "missp"      : int (splitted_line   [missp_idx]     .split("M")[1]),
            "bw"         : int(splitted_line    [bw_idx]        .split('B')[1]), 
            "uInterval"  : int(splitted_line    [uInterval_idx] .split('U')[1]), 
            "alg_mode"   : splitted_line        [alg_idx]       .split(" ")[0],
            "cost"       : cost
            }

    def print_tbl (self):
        """
        Print table of service costs, normalized w.r.t. to Opt, in tikz format
        """
        self.tbl_output_file    = open ("../res/tbl.dat", "w")
        traces = ['wiki', 'gradle', 'scarab', 'umass']

        printf (self.tbl_output_file, '\tMiss Penalty & Policy ')
        for trace in traces:
            trace_to_print = 'F2' if trace == 'umass' else trace 
            printf (self.tbl_output_file, '& {}' .format (trace_to_print))
        printf (self.tbl_output_file, '\\\\\n\t\\hline\n\t\\hline\n')

        self.gen_filtered_list(self.list_of_dicts, num_of_req = 1000) 
        for missp in [40, 400, 4000]:
            printf (self.tbl_output_file, '\t\\multirow{3}{*}{')
            printf (self.tbl_output_file, '{}' .format (missp))
            printf (self.tbl_output_file, '}\n')
            for alg_mode in ['FNO', 'FNA']:
                if (alg_mode == 'FNO'):
                    printf (self.tbl_output_file, '\t&$\\fno$' .format(alg_mode))
                if (alg_mode == 'FNA'):
                    printf (self.tbl_output_file, '\t&$\\fna$' .format(alg_mode))
                    
                for trace in traces:
                    opt_cost = self.gen_filtered_list(self.list_of_dicts, 
                                                              trace = trace, cache_size = 10, num_of_DSs = 3, Kloc = 1, 
                                                              missp = missp, alg_mode = 'Opt')[0]['cost']
                    alg_cost = self.gen_filtered_list(self.list_of_dicts, 
                                                              trace = trace, cache_size = 10, bpe = 14, num_of_DSs = 3, Kloc = 1, 
                                                              missp = missp, uInterval = 1000, alg_mode = alg_mode)[0]['cost']
                    printf (self.tbl_output_file, ' & {:.4f}' .format(alg_cost / opt_cost))
                printf (self.tbl_output_file, ' \\\\\n')
            printf (self.tbl_output_file, '\t\\hline\n\n')
                

    def gen_filtered_list (self, list_to_filter, trace = None, cache_size = 0, bpe = 0, num_of_DSs = 0, Kloc = 0, missp = 0, uInterval = 0, 
                           num_of_req = 0, alg_mode = None):
        """
        filters and takes from all the items in a given list (that was read from the res file) only those with the desired parameters value
        The function filters by some parameter only if this parameter is given an input value > 0.
        E.g.: 
        If bpe == 0, the function discards bpe values, and doesn't filter out entries by their bpe values.
        If bpe == 5, the function returns only entries in which bpe == 5.      
        """
        if (not (trace == None)):
            list_to_filter = list (filter (lambda item : item['trace'] == trace, list_to_filter))
        if (cache_size > 0):
            list_to_filter = list (filter (lambda item : item['cache_size'] == cache_size, list_to_filter))
        if (bpe > 0):
            list_to_filter = list (filter (lambda item : item['bpe'] == bpe, list_to_filter))
        if (num_of_DSs > 0):
            list_to_filter = list (filter (lambda item : item['num_of_DSs'] == num_of_DSs, list_to_filter))
        if (Kloc > 0):
            list_to_filter = list (filter (lambda item : item['Kloc'] == Kloc, list_to_filter))
        if (missp > 0):
            list_to_filter = list (filter (lambda item : item['missp'] == missp, list_to_filter))
        if (uInterval > 0):
            list_to_filter = list (filter (lambda item : item['uInterval'] == uInterval, list_to_filter))
        if (num_of_req > 0):
            list_to_filter = list (filter (lambda item : item['num_of_req'] == num_of_req, list_to_filter))
        if (not (alg_mode == None)):
            list_to_filter = list (filter (lambda item : item['alg_mode'] == alg_mode, list_to_filter))
        return list_to_filter

    def print_single_tikz_plot (self, list_of_dict, key_to_sort, addplot_str = None, add_legend_str = None, legend_entry = None):
        """
        Prints a single plot in a tikz format.
        Inputs:
        The "x" value is the one which the user asks to sort the inputs (e.g., "x" value may be the cache size, uInterval, etc).
        The "y" value is the cost for this "x". 
        list_of_dicts - a list of Python dictionaries. 
        key_to_sort - the function sorts the items by this key, e.g.: cache size, uInterval, etc.
        addplot_str - the "add plot" string to be added before each list of points (defining the plot's width, color, etc.).
        addlegend_str - the "add legend" string to be added after each list of points.
        legend_entry - the entry to be written (e.g., 'Opt', 'Alg', etc).
        """
        if (not (addplot_str == None)):
            printf (self.output_file, addplot_str)
        for dict in sorted (list_of_dict, key = lambda i: i[key_to_sort]):
            printf (self.output_file, '({:.0f}, {:.04f})' .format (dict[key_to_sort], dict['cost']))
        printf (self.output_file, self.end_add_plot_str)
        if (not (add_legend_str == None)): # if the caller requested to print an "add legend" str          
            printf (self.output_file, '\t\t{}{}' .format (self.add_legend_str, legend_entry))    
            printf (self.output_file, '}\n')    
        printf (self.output_file, '\n\n')    
        
        
    def print_cache_size_plot_normalized (self):
        """
        Print a tikz plot of the service cost as a func' of the bpe
        """    
        opt_list     = sorted (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'Opt'), key = lambda i: i['cache_size']) 

        add_legend_str = None
        for uInterval in [256, 1024]:
            if (uInterval == 1024):
                add_legend_str = self.add_legend_str
            printf (self.output_file, '%% uInterval = {}\n' .format (uInterval))
            for alg_mode in ['FNO', 'FNA']:
                filtered_list  = self.gen_filtered_list(self.list_of_dicts, num_of_DSs = 3, Kloc = 1, missp = 100, 
                                                        alg_mode = alg_mode, uInterval = uInterval)
                for dict in filtered_list: 
    
                     dict['cost'] /= self.gen_filtered_list (opt_list, cache_size = dict['cache_size'])[0]['cost'] # normalize the cost w.r.t. Opt
     
                self.print_single_tikz_plot (filtered_list, key_to_sort = 'cache_size', addplot_str = self.add_plot_str_dict[alg_mode], 
                                             add_legend_str = add_legend_str,    legend_entry = self.legend_entry_dict[alg_mode]) 

       
    def print_cache_size_plot (self):
        """
        Print a tikz plot of the service cost as a func' of the bpe
        """    
        filtered_list = self.gen_filtered_list (self.list_of_dicts, bpe = 14, missp = 100) # Filter only relevant from the results file  
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'Opt'), 
                                     'cache_size', addplot_str = self.add_plot_opt, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'Opt') 

        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 16), 
                                     'cache_size', addplot_str = self.add_plot_str1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, FNA, uInterval = 16') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 256), 
                                     'cache_siz e', addplot_str = self.add_plot_fno1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, uInterval = 256') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNA', uInterval = 256), 
                                     'cache_size', addplot_str = self.add_plot_fna1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNA, uInterval = 256') 

        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 1024), 
                                     'cache_size', addplot_str = self.add_plot_fno2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, uInterval = 1024') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNA', uInterval = 1024), 
                                     'cache_size', addplot_str = self.add_plot_fna2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNA, uInterval = 1024') 
        
    def print_bpe_plot (self):
        """
        Print a tikz plot of the service cost as a func' of the bpe
        """    
        filtered_list = self.gen_filtered_list (self.list_of_dicts, cache_size = 10, missp = 100) # Filter only relevant from the results file  
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'Opt'), 
                                     'bpe', addplot_str = self.add_plot_opt, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'Opt') 

        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 16), 
                                     'bpe', addplot_str = self.add_plot_str1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, FNA, uInterval = 16') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 256), 
                                     'bpe', addplot_str = self.add_plot_fno1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, uInterval = 256') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNA', uInterval = 256), 
                                     'bpe', addplot_str = self.add_plot_fna1, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNA, uInterval = 256') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO', uInterval = 1024), 
                                     'bpe', addplot_str = self.add_plot_fno2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO, uInterval = 1024') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNA', uInterval = 1024), 
                                     'bpe', addplot_str = self.add_plot_fna2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNA, uInterval = 1024') 
        
    def print_uInterval_plot (self):
        """
        Print a tikz plot of the service cost as a func' of the update interval.
        The print shows Opt, FNO, and FNA
        """    
        filtered_list = self.gen_filtered_list (self.list_of_dicts, cache_size = 10, missp = 100, bpe = 14) # Filter only relevant from the results file  
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'Opt'), 
                                     'uInterval', addplot_str = self.add_plot_opt, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'Opt') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNO'), 
                                     'uInterval', addplot_str = self.add_plot_fno2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNO') 
        
        self.print_single_tikz_plot (self.gen_filtered_list (self.list_of_dicts, alg_mode = 'FNA'), 
                                     'uInterval', addplot_str = self.add_plot_fna2, 
                                     add_legend_str = self.add_legend_str, legend_entry = 'FNA') 
                
    def print_normalized_plot (self, key_to_sort, uInterval = 0, print_add_legend = True):
        """
        Print a tikz plot of the service cost as a func' of the update interval
        The print shows FNO and FNA, both normalized w.r.t. Opt
        """    
        filtered_list = self.gen_filtered_list (self.list_of_dicts, cache_size = 10, missp = 100, bpe = 14) # Filter only relevant from the results file  
        opt_cost = self.gen_filtered_list(self.list_of_dicts, cache_size = 10, num_of_DSs = 3, Kloc = 1, missp = 100, alg_mode = 'Opt')[0]['cost']

        if (uInterval > 0 ):
            printf (self.output_file, '%% uInterval = {}\n' .format (uInterval))
        for alg_mode in ['FNO', 'FNA']:
            
            filtered_list  = self.gen_filtered_list(self.list_of_dicts, uInterval = uInterval, cache_size = 10, num_of_DSs = 3, Kloc = 1, missp = 100, alg_mode = alg_mode)
            add_legend_str = self.add_legend_str if print_add_legend else None
            for dict in filtered_list: 
                dict['cost'] /= opt_cost

            self.print_single_tikz_plot (filtered_list, key_to_sort = key_to_sort, addplot_str = self.add_plot_str_dict[alg_mode], 
                                         add_legend_str = add_legend_str,    legend_entry = self.legend_entry_dict[alg_mode]) 
            
                    
    def parse_file (self, input_file_name):
    
        self.input_file         = open ("../res/" + input_file_name,  "r")
        self.output_file        = open ("../res/" + input_file_name.split(".")[0] + ".dat", "w")
        lines               = (line.rstrip() for line in self.input_file) # "lines" contains all lines in input file
        lines               = (line for line in lines if line)       # Discard blank lines
        self.list_of_dicts  = []
        
        for line in lines:
        
            # Discard lines with comments / verbose data
            if (line.split ("//")[0] == ""):
                continue
           
            self.parse_line(line)
            self.list_of_dicts.append(self.dict)
                

        cache_size = 10 # cache size to plot, in units of [K] entries        
        uInterval  = 1000
        alg_modes = ['FNA', 'FNO']
        self.input_file.close
        
    
if __name__ == "__main__":
    my_Res_file_parser = Res_file_parser ()
                
#     my_Res_file_parser.parse_file ('wiki_cache_size.res') 
#     my_Res_file_parser.print_cache_size_plot_normalized ()

# my_Res_file_parser.parse_file ('gradle_bpe.res') 
# my_Res_file_parser.print_normalized_plot('bpe', uInterval = 256, print_add_legend = False)
# my_Res_file_parser.print_normalized_plot('bpe', uInterval = 1024, print_add_legend = True)



#     for trace in ['wiki']:
#         for op in ['bpe']:
#             my_Res_file_parser.parse_file (trace + '_' + op + '.res') #("wiki_uInterval.res")
#             if (op == 'uInterval'):
#                 my_Res_file_parser.print_uInterval_plot ()
#             if (op == 'bpe'):
#                 my_Res_file_parser.print_bpe_plot ()
#             if (op == 'cache_size'):
#                 my_Res_file_parser.print_cache_size_plot ()
