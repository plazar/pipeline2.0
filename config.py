###############################################################
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

################################################################
# Import search module and set parameters
################################################################
def init_search():
    """Import search module and set parameters.
        Return Module object that contains search's main function.
        The main function should have the following signature:
            main(filename, working_directory)
    """
    import PALFA2_presto_search as search
    
    # Tunable parameters for searching and folding
    # (you probably don't need to tune any of them)
    search.datatype_flag           = "-psrfits" # PRESTO flag to determine data type
    search.rfifind_chunk_time      = 2**15 * 0.000064  # ~2.1 sec for dt = 64us
    search.singlepulse_threshold   = 5.0  # threshold SNR for candidate determination
    search.singlepulse_plot_SNR    = 6.0  # threshold SNR for singlepulse plot
    search.singlepulse_maxwidth    = 0.1  # max pulse width in seconds
    search.to_prepfold_sigma       = 6.0  # incoherent sum significance to fold candidates
    search.max_cands_to_fold       = 50   # Never fold more than this many candidates
    search.numhits_to_fold         = 2    # Number of DMs with a detection needed to fold
    search.low_DM_cutoff           = 2.0  # Lowest DM to consider as a "real" pulsar
    search.lo_accel_numharm        = 16   # max harmonics
    search.lo_accel_sigma          = 2.0  # threshold gaussian significance
    search.lo_accel_zmax           = 0    # bins
    search.lo_accel_flo            = 2.0  # Hz
    search.hi_accel_numharm        = 8    # max harmonics
    search.hi_accel_sigma          = 3.0  # threshold gaussian significance
    search.hi_accel_zmax           = 50   # bins
    search.hi_accel_flo            = 1.0  # Hz
    search.low_T_to_search         = 20.0 # sec
    
    # Sifting specific parameters (don't touch without good reason!)
    search.sifting.sigma_threshold = search.to_prepfold_sigma-1.0  # incoherent power threshold (sigma)
    search.sifting.c_pow_threshold = 100.0                  # coherent power threshold
    search.sifting.r_err           = 1.1    # Fourier bin tolerence for candidate equivalence
    search.sifting.short_period    = 0.0005 # Shortest period candidates to consider (s)
    search.sifting.long_period     = 15.0   # Longest period candidates to consider (s)
    search.sifting.harm_pow_cutoff = 8.0    # Power required in at least one harmonic

    return search
