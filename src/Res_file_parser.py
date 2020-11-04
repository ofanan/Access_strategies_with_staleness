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

    
    def __init__ (self, input_file_name):
        self.input_file  = open ("../res/" + input_file_name,  "r")
        # self.output_file_name    = input_file_name.split(".")[0] + ".res"
        output_file = open ("../res/" + input_file_name.split(".")[0] + ".res", "a")  
    
def parse_line (self, line):
    
     self.settings = splitted_line[0]
     cost          = splitted_line[1]
     self.splitted_line = self.settings.split (".")
     self.trace      =      self.splitted_line[trace_idx] 
     self.cache_size = int (self.splitted_line[cache_size_idx].split("C")[1])   == 'C' 
     print (self.trace)
     print (self.cache_size)
     
     if len (self.splitted_line) < num_of_fields:
        print ("encountered a format error")
        return False
#         self.bpe        = int (splitted_line[bpe_idx]
#         self.num_of_req = splitted_line[num_of_req_idx]
#         self.num_of_DSs = splitted_line[num_of_DSs_idx] 
#         self.kloc       = splitted_line[kloc_idx]
#         self.missp      = splitted_line[missp_idx]
#         self.bw         = splitted_line[bw_idx]           == 'B'        + bw          and 
#         self.alg_mode   = splitted_line[alg_idx].split(" ")[0]
    return True

def is_requested_entry (line, trace = "gradle", cache_size  = "10K", bpe = "14", num_of_req  = "750", 
                   num_of_DSs  = "3", kloc = "1", missp = "100", bw = "10", alg_mode = "Opt"):
    splitted_line = line.split (".")
    
    if len (splitted_line) < num_of_fields:
        print ("encountered a format error")
        return False
    if (self.settings[trace_idx]        ==              trace       and 
        self.settings[cache_size_idx]   == 'C'        + cache_size  and 
        self.settings[bpe_idx]          == 'bpe'      +  bpe        and
        self.settings[num_of_req_idx]   == num_of_req + 'Kreq'      and
        self.settings[num_of_DSs_idx]   == num_of_DSs + 'DSs'       and 
        self.settings[kloc_idx]         == 'Kloc'     + kloc        and 
        self.settings[missp_idx]        == 'M'        + missp       and
        self.settings[bw_idx]           == 'B'        + bw          and 
        self.settings[alg_idx].split(" ")[0] ==         alg_mode):
        return True
    return False

def parse_file (self):

    lines = (line.rstrip() for line in self.input_file) # "lines" contains all lines in input file
    lines = (line for line in lines if line)       # Discard blank lines
    
    # BWs = [20, 40, 60, 80, 100, 120, 140, 160, 180, 200]    
    # alg_modes = ["FNA"]
    
    for line in lines:
    
        # Discard lines with comments / verbose data
        if (line.split ("//")[0] == ""):
            continue
        
            
        for alg_mode in alg_modes:
            for bw in BWs:
                if (is_requested_entry (settings, bw = str(bw), alg_mode = alg_mode)):
                    cost     = splitted_line[1].split ("=")[1]
                    printf (output_file, '({:.0f}, {:.4f}) ' .format(bw, float(cost)))
    input_file.close
    

