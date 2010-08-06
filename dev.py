import os

incoming_file_dir = "/homes/borgii/snipka/FTP"

def generate_dummy_fits():
    #rawdata_re_pattern = r"^p2030.*b[0-7]s[01]g?.*\.fits$"
    for i in range(5):
        f = open(incoming_file_dir+"/"+"p2030_"+str(i)+"_b0s0g_"+str(i)+".fits",'w')
        f.close
    return True

def add_files():
    for i in range(6,11):
        f = open(incoming_file_dir+"/"+"p2030_"+str(i)+"_b0s0g_"+str(i)+".fits",'w')
        f.close
    return True