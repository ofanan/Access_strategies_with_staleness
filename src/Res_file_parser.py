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

    def parse_line (self, line):
        splitted_line = line.split ("|")
         
        settings        = splitted_line[0]
        cost            = float(splitted_line[1].split(" = ")[1])
        splitted_line   = settings.split (".")

        if len (splitted_line) < num_of_fields:
            print ("encountered a format error")
            return False
#         print ('splitted line = ', splitted_line)
#         print ('cache_size = ', splitted_line[cache_size_idx].split("C")[1].split("K")[0])
        self.dict = {
            "trace"      : splitted_line[trace_idx],
            "cache_size" : int (splitted_line[cache_size_idx].split("C")[1].split("K")[0]),   
            "bpe"        : int (splitted_line[bpe_idx].split("bpe")[1]),
            "num_of_req" : splitted_line[num_of_req_idx].split("req")[0],
            "num_of_DSs" : int (splitted_line[num_of_DSs_idx].split("DSs")[0]), 
            "Kloc"       : int (splitted_line[kloc_idx].split("Kloc")[1]),
            "missp"      : int (splitted_line[missp_idx].split("M")[1]),
            "bw"         : int(splitted_line[bw_idx].split('B')[1]), 
            "uInterval"  : int(splitted_line[uInterval_idx].split('U')[1]), 
            "alg_mode"   : splitted_line[alg_idx].split(" ")[0],
            "cost"       : cost
            }

    def print_tikz_line (self, list_of_dict, key_to_sort, legend_entry):
        for dict in sorted (list_of_dict, key = lambda i: i[key_to_sort]):
            printf (self.output_file, '({:.0f} ,{:.04f})' .format (dict[key_to_sort], dict['cost']))
        printf (self.output_file, '\n};\n\\addlegendentry {')
        printf (self.output_file, legend_entry)
        printf (self.output_file, '}\n\n')
        
        

    def print_to_tikz (self, key_to_sort, alg_mode):
        if (alg_mode == 'FNA'):
            printf (self.output_file, '\\addplot[color=black,     mark=triangle,     width = \plotwidth] coordinates {\n')
            self.print_tikz_line (self.list_of_dicts_FNA, key_to_sort, 'FNA')
        else:
            printf (self.output_file, '\\addplot[color=red,          mark=o,                 width = \plotwidth] coordinates {\n')
            self.print_tikz_line (self.list_of_dicts_FNO, key_to_sort, 'FNO')             

    def parse_file (self, input_file_name):
    
        self.input_file  = open ("../res/" + input_file_name,  "r")
        self.output_file = open ("../res/" + input_file_name.split(".")[0] + ".dat", "a")  
        lines = (line.rstrip() for line in self.input_file) # "lines" contains all lines in input file
        lines = (line for line in lines if line)       # Discard blank lines
        self.list_of_dicts_FNA = []
        self.list_of_dicts_FNO = []
        
        for line in lines:
        
            # Discard lines with comments / verbose data
            if (line.split ("//")[0] == ""):
                continue
           
            self.parse_line(line)
            
            if (self.dict["alg_mode"] == 'FNA'):
                self.list_of_dicts_FNA.append(self.dict)
            else:
                self.list_of_dicts_FNO.append(self.dict)
                
        
        self.print_to_tikz ('uInterval', 'FNA')
        self.print_to_tikz ('uInterval', 'FNO')
 
        self.input_file.close
        
    
