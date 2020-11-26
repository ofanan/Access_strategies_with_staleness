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
        self.add_legend_str = '\n};\n\\addlegendentry {'
        self.add_plot_str_fna = '\\addplot[color=black,     mark=triangle,     width = \plotwidth] coordinates {\n'
        self.add_plot_str_fno = '\\addplot[color=red,          mark=o,                 width = \plotwidth] coordinates {\n'

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

    def print_table (self):
        traces          = ['wiki'] #['gradle', 'wiki', 'scarab', 'F2']
        alg_modes       = ['FNO', 'FNA']
        miss_penalties  = [40, 400, 4000, 100]
        for missp in miss_penalties:
            for alg_mode in alg_modes:
                for trace in traces:
                    relevant_dicts = self.gen_filtered_list(self.list_of_dicts, 
                                                              trace = trace, cache_size = 10, bpe = 14, num_of_DSs = 3, Kloc = 1, 
                                                              missp = missp, uInterval = 1024, alg_mode = alg_mode)
                    for dict in relevant_dicts:
                        printf (self.tbl_output_file, '{:.4f}' .format(dict ['cost']))

    def gen_filtered_list (self, list_to_filter, trace = None, cache_size = 0, bpe = 0, num_of_DSs = 0, Kloc = 0, missp = 0, uInterval = 0, alg_mode = None):
        """
        filters and takes from all the items in a given list (that was read from the res file) only those with the desired parameters value
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
        if (not (alg_mode == None)):
            list_to_filter = list (filter (lambda item : item['alg_mode'] == alg_mode, list_to_filter))
        return list_to_filter

    def print_single_tikz_plot (self, list_of_dict, key_to_sort, addplot_str = None, add_legend_str = None, legend_entry = None):
        if (not (addplot_str == None)):
            printf (self.output_file, addplot_str)
        for dict in sorted (list_of_dict, key = lambda i: i[key_to_sort]):
            printf (self.output_file, '({:.0f}, {:.04f})' .format (dict[key_to_sort], dict['cost']))
        if (not (add_legend_str == None)): # if the caller requested to print an "add legend" str          
            printf (self.output_file, '{}{}' .format (self.add_legend_str, legend_entry))    
            printf (self.output_file, '}\n\n')    
        
    def parse_file (self, input_file_name):
    
        self.input_file         = open ("../res/" + input_file_name,  "r")
        self.output_file        = open ("../res/" + input_file_name.split(".")[0] + ".dat", "w")
        self.tbl_output_file    = open ("../res/tbl.dat", "w")
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
        uInterval  = 128
        self.print_table()
        exit ()
        alg_modes = ['FNA', 'FNO']
        for alg_mode in alg_modes:
            self.print_single_tikz_plot (self.gen_filtered_list(self.list_of_dicts, alg_mode = alg_mode, uInterval = uInterval),
                                     'cache_size', addplot_str = self.add_plot_str_fna, 
                                     add_legend_str = self.add_legend_str, legend_entry = alg_mode) #, cache_size = cache_size)
 
        self.input_file.close
        
    
