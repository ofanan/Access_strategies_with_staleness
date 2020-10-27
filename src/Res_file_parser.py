from printf import printf 

input_file  = open ("../res/res.txt", "r")
output_file = open ("../res/res.dat", "a")

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

def is_requested_entry (line, trace = "gradle", cache_size  = "400", bpe = "14", num_of_req  = "50", 
                   num_of_DSs  = "3", kloc = "1", missp = "100", bw = "10", alg = "Opt"):
    splitted_line = line.split (".")
    
    if len (splitted_line) < num_of_fields:
        print ("encountered a format error")
        return False
    if (splitted_line[trace_idx]        ==              trace       and 
        splitted_line[cache_size_idx]   == 'C'        + cache_size  and 
        splitted_line[bpe_idx]          == 'bpe'      +  bpe        and
        splitted_line[num_of_req_idx]   == num_of_req + 'Kreq'      and
        splitted_line[num_of_DSs_idx]   == num_of_DSs + 'DSs'       and 
        splitted_line[kloc_idx]         == 'Kloc'     + kloc        and 
        splitted_line[missp_idx]        == 'M'        + missp       and
        splitted_line[bw_idx]           == 'B'        + bw          and 
        splitted_line[alg_idx].split(" ")[0] ==         alg):
        return True
    return False

lines = (line.rstrip() for line in input_file) # "lines" contains all lines in input file
lines = (line for line in lines if line)       # Discard blank lines

BWs = [4, 8]    
for line in lines:
    # Discard lines with comments / verbose data
    if (line.split ("//")[0] == ""):
        continue
    
    splitted_line = line.split ("|")
    settings = splitted_line[0]
    cost     = splitted_line[1]
    
    for bw in BWs:
        if (is_requested_entry (settings, bw = str(bw))):
            printf (output_file, '({:.0f}, {}), ' .format(bw, cost))
input_file.close
    
