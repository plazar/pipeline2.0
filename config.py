################################################################
# Basic parameters
################################################################
institution = "McGill"
pipeline = "PRESTO"
survey = "PALFA2.0"

################################################################
# Configurations for processing
################################################################
base_working_directory = "/scratch/PALFA/"
zaplist = "/homes/borgii/plazar/research/PALFA/pipeline2.0/PALFA.zaplist"
log_dir = "/homes/borgii/snipka/dev/pipeline2.0/log/"
max_jobs_running = 10
job_basename = "%s_batchjob" % survey
sleep_time = 10*60 # time to sleep between submitting jobs (in seconds)
max_attempts = 2 # Maximum number of times a job is attempted due to errors
#resource_list = "nodes=borg94:ppn=1:JumboFrame" # resource list for PBS's qsub
resource_list = "nodes=borg92:ppn=1" # resource list for PBS's qsub
delete_rawdata = False
################################################################
# Configurations for raw data
################################################################
rawdata_directory = "/data/alfa/FTP"
#rawdata_directory = "C:/Reposotories/data/"
rawdata_re_pattern = r"^p2030.*b[0-7]s[01]g?.*\.fits$"

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
    
    # Sifting specific parameters (don't touch without good reason!)
    presto_search.sifting.sigma_threshold = presto_search.to_prepfold_sigma-1.0  
                                                   # incoherent power threshold (sigma)
    presto_search.sifting.c_pow_threshold = 100.0  # coherent power threshold
    presto_search.sifting.r_err           = 1.1    # Fourier bin tolerence for candidate equivalence
    presto_search.sifting.short_period    = 0.0005 # Shortest period candidates to consider (s)
    presto_search.sifting.long_period     = 15.0   # Longest period candidates to consider (s)
    presto_search.sifting.harm_pow_cutoff = 8.0    # Power required in at least one harmonic

    return presto_search
