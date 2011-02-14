from config_defs import basic, processing, background, email, download, upload, commondb
################################################################
# Basic parameters
################################################################
basic.institution = "McGill"
basic.pipeline = "PRESTO"
basic.survey = "PALFA2.0"
basic.pipelinedir = "/homes/borgii/plazar/research/PALFA/pipeline2.0_clean/pipeline2.0"

################################################################
# Configurations for processing
################################################################
processing.base_results_directory = "/data/data7/PALFA/test_new_pipeline_clean/"
processing.base_working_directory = "/exports/scratch/PALFA/"
processing.default_zaplist = "/homes/borgii/plazar/research/PALFA/pipeline2.0/lib/zaplists/PALFA.zaplist"
processing.zaplistdir = "/homes/borgii/plazar/research/PALFA/pipeline2.0/lib/zaplists/"

processing.max_jobs_running = 50
processing.job_basename = "%s_batchjob" % basic.survey
processing.max_attempts = 2 # Maximum number of times a job is attempted due to errors
processing.resource_list = "nodes=P248:ppn=1" # resource list for PBS's qsub
processing.delete_rawdata = True
import QsubManager
processing.QueueManagerClass = QsubManager.Qsub()
################################################################
# Import presto_search module and set parameters
################################################################
def init_presto_search():
    """Import search module and set parameters.
        Return Module object that contains search's main function.
        The main function should have the following signature:
            main(filename, working_directory)
    """
    import PALFA2_presto_search as presto_search
    
    # The following determines if we'll dedisperse and fold using subbands.
    # In general, it is a very good idea to use them if there is enough scratch
    # space on the machines that are processing (~30GB/beam processed)
    presto_search.use_subbands          = True
    # To fold from raw data (ie not from subbands or dedispersed FITS files)
    # set the following to True.
    presto_search.fold_rawdata          = True
    
    # Tunable parameters for searching and folding
    # (you probably don't need to tune any of them)
    presto_search.datatype_flag           = "-psrfits" # PRESTO flag to determine data type
    presto_search.rfifind_chunk_time      = 2**15 * 0.000064  # ~2.1 sec for dt = 64us
    presto_search.singlepulse_threshold   = 5.0  # threshold SNR for candidate determination
    presto_search.singlepulse_plot_SNR    = 6.0  # threshold SNR for singlepulse plot
    presto_search.singlepulse_maxwidth    = 0.1  # max pulse width in seconds
    presto_search.to_prepfold_sigma       = 6.0  # incoherent sum significance to fold candidates
    presto_search.max_cands_to_fold       = 50   # Never fold more than this many candidates
    presto_search.numhits_to_fold         = 2    # Number of DMs with a detection needed to fold
    presto_search.low_DM_cutoff           = 2.0  # Lowest DM to consider as a "real" pulsar
    presto_search.lo_accel_numharm        = 16   # max harmonics
    presto_search.lo_accel_sigma          = 2.0  # threshold gaussian significance
    presto_search.lo_accel_zmax           = 0    # bins
    presto_search.lo_accel_flo            = 2.0  # Hz
    presto_search.hi_accel_numharm        = 8    # max harmonics
    presto_search.hi_accel_sigma          = 3.0  # threshold gaussian significance
    presto_search.hi_accel_zmax           = 50   # bins
    presto_search.hi_accel_flo            = 1.0  # Hz
    presto_search.low_T_to_search         = 20.0 # sec

    # DDplan configurations
    # The following configurations are for calculating dedispersion plans 
    # on demand. Currently dedispersion plans for WAPP and Mock data 
    # are hardcoded.
    # presto_search.lodm        = 0      # pc cm-3
    # presto_search.hidm        = 1000   # pc cm-3
    # presto_search.resolution  = 0.1    # ms
    # if presto_search.use_subbands:
    #     presto_search.numsub  = 96     # subbands
    # else:
    #     presto_search.numsub  = 0      # Defaults to number of channels

    # Sifting specific parameters (don't touch without good reason!)
    presto_search.sifting.sigma_threshold = presto_search.to_prepfold_sigma-1.0  
                                                   # incoherent power threshold (sigma)
    presto_search.sifting.c_pow_threshold = 100.0  # coherent power threshold
    presto_search.sifting.r_err           = 1.1    # Fourier bin tolerence for candidate equivalence
    presto_search.sifting.short_period    = 0.0005 # Shortest period candidates to consider (s)
    presto_search.sifting.long_period     = 15.0   # Longest period candidates to consider (s)
    presto_search.sifting.harm_pow_cutoff = 8.0    # Power required in at least one harmonic

    return presto_search


################################################################
# Result Uploader Configuration
################################################################
import subprocess
import imp
import os
prestodir = os.getenv('PRESTO')
prestohash = subprocess.Popen("git rev-parse HEAD", cwd=prestodir, \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
pipelinehash = subprocess.Popen("git rev-parse HEAD", cwd=basic.pipelinedir, \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
upload.version_num = 'PRESTO:%s;PIPELINE:%s' % (prestohash, pipelinehash)


################################################################
# Common Database Configuration
################################################################
commondb.username = 'mcgill'
commondb.password = 'pw4sd2mcgill!'
commondb.host = 'arecibosql.tc.cornell.edu'


################################################################
# Background Script Configuration
################################################################
background.screen_output = True # Set to True if you want the script to 
                                # output runtime information, False otherwise
# Path to sqlite3 database file
background.jobtracker_db = "/data/alfa/test_pipeline_clean/storage_db"

################################################################
# Email Notification Configuration
################################################################
email.enabled = True
email.smtp_host = 'smtp.gmail.com' # None - For use of the local smtp server
email.smtp_username = 'mcgill.pipeline@gmail.com'
email.smtp_password = 'mcg1592l!!'
email.recipient = 'patricklazarus@gmail.com' # The address to send emails to
email.sender = None # From address to show in email
email.send_on_failures = True
email.send_on_terminal_failures = True


################################################################
# Downloader Configuration
################################################################
download.api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL"
download.api_username = "mcgill"
download.api_password = "palfa@Mc61!!"
download.temp = "/data/alfa/test_pipeline_clean/"
download.space_to_use = 228748364800
download.numdownloads = 2
download.numrestores = 2
download.numretries = 3
