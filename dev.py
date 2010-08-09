import os
import time
import string
import config


incoming_file_dir = config.rawdata_directory

def generate_dummy_fits():
    #rawdata_re_pattern = r"^p2030.*b[0-7]s[01]g?.*\.fits$"
    for i in range(5):
        f = open(os.path.join(incoming_file_dir,"p2030_"+str(i)+"_b0s0g_"+str(i)+".fits"),'w')
        f.close
    return True

def add_files():
    for i in range(6,11):
        f = open(os.path.join(incoming_file_dir,"p2030_"+str(i)+"_b0s0g_"+str(i)+".fits"),'w')
        f.close
    return True

def get_fake_job_id():
    return string.replace(str(time.time()),'.','') + ".borgii-gw.beowulf.borgs"

def write_fake_qsub_error(jobid):
    fake_f = open(jobid,'w');
    fake_f.write("FAKE ERROR FAKE ERROR")
    fake_f.close()