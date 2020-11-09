from printf import printf 

trace_idx       = 0
cache_size_idx  = 1
bpe_idx         = 2
num_of_req_idx  = 3
num_of_DSs_idx  = 4
kloc_idx        = 5
missp_idx       = 6
bw_idx          = 7
alg_idx         = 8
num_of_fields   = 9

trace       = "gradle"
cache_size  = "40"
bpe         = "14"
num_of_req  = "500"
num_of_DSs  = "3"
kloc        = "1"
missp       = "100" 
bw          = "10"

class Res_file_parser (object):

    
#     def __init__ (self, input_file_name):
#         self.input_file  = open ("../res/" + input_file_name,  "r")
#         self.output_file = open ("../res/" + input_file_name.split(".")[0] + ".dat", "a")  
    
# def is_requested_entry (line, trace = "gradle", cache_size  = "10K", bpe = "14", num_of_req  = "750", 
#                    num_of_DSs  = "3", kloc = "1", missp = "100", bw = "10", alg_mode = "Opt"):
#     splitted_line = line.split (".")
#     
#     if len (splitted_line) < num_of_fields:
#         print ("encountered a format error")
#         return False
#     if (self.settings[trace_idx]        ==              trace       and 
#         self.settings[cache_size_idx]   == 'C'        + cache_size  and 
#         self.settings[bpe_idx]          == 'bpe'      +  bpe        and
#         self.settings[num_of_req_idx]   == num_of_req + 'Kreq'      and
#         self.settings[num_of_DSs_idx]   == num_of_DSs + 'DSs'       and 
#         self.settings[kloc_idx]         == 'Kloc'     + kloc        and 
#         self.settings[missp_idx]        == 'M'        + missp       and
#         self.settings[bw_idx]           == 'B'        + bw          and 
#         self.settings[alg_idx].split(" ")[0] ==         alg_mode):
#         return True
#     return False

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
            "trace"        : splitted_line[trace_idx],
            "cache_size"   : int (splitted_line[cache_size_idx].split("C")[1].split("K")[0]),   
            "bpe"        : int (splitted_line[bpe_idx].split("bpe")[1]),
            "num_of_req" : splitted_line[num_of_req_idx].split("req")[0],
            "num_of_DSs" : int (splitted_line[num_of_DSs_idx].split("DSs")[0]), 
            "Kloc"       : int (splitted_line[kloc_idx].split("Kloc")[1]),
            "missp"      : int (splitted_line[missp_idx].split("M")[1]),
            "bw"         : int(splitted_line[bw_idx].split('B')[1]), 
            "alg_mode"   : splitted_line[alg_idx].split(" ")[0],
            "cost"       : cost
            }

    def print_tikz_line (self, list_of_dict, key_to_sort, legend_entry):
        for dict in sorted (list_of_dict, key = lambda i: i[key_to_sort]):
            printf (self.output_file, '({:.0f} ,{:.04f})' .format (dict['bw'], dict['cost']))
        printf (self.output_file, '\n};\\addlegendentry {')
        printf (self.output_file, legend_entry)
        printf (self.output_file, '}\n\n')
        
        

    def print_to_tikz (self, alg_mode):
        if (alg_mode == 'FNA'):
            printf (self.output_file, '\\addplot[color=black,     mark=triangle,     width = \plotwidth] coordinates {\n')
            self.print_tikz_line (self.list_of_dicts_FNA, 'bw', 'FNA')
        else:
            printf (self.output_file, '\\addplot[color=red,          mark=o,                 width = \plotwidth] coordinates {\n')
            self.print_tikz_line (self.list_of_dicts_FNO, 'bw', 'FNO')             

    def parse_file (self, input_file_name):
    
        self.input_file  = open ("../res/" + input_file_name,  "r")
        self.output_file = open ("../res/" + input_file_name.split(".")[0] + ".dat", "a")  
        lines = (line.rstrip() for line in self.input_file) # "lines" contains all lines in input file
        lines = (line for line in lines if line)       # Discard blank lines
        self.list_of_dicts_FNA = []
        self.list_of_dicts_FNO = []
        
        # BWs = [20, 40, 60, 80, 100, 120, 140, 160, 180, 200]    
        # alg_modes = ["FNA"]
        
        for line in lines:
        
            # Discard lines with comments / verbose data
            if (line.split ("//")[0] == ""):
                continue
           
            self.parse_line(line)
            
            if (self.dict["alg_mode"] == 'FNA'):
                self.list_of_dicts_FNA.append(self.dict)
            else:
                self.list_of_dicts_FNO.append(self.dict)
                
        
        self.print_to_tikz ('FNA')
        self.print_to_tikz ('FNO')
 
        self.input_file.close
        
    
