import os

def getTracesPath():
	trace_path_splitted = os.getcwd().split ("\\")
	return (trace_path_splitted[0] + "/" + trace_path_splitted[1] + "/" + trace_path_splitted[2] + "/Documents/traces/") 
	