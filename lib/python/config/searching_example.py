# The following determines if we'll dedisperse and fold using subbands.
# In general, it is a very good idea to use them if there is enough scratch
# space on the machines that are processing (~30GB/beam processed)
use_subbands          = True
# To fold from raw data (ie not from subbands or dedispersed FITS files)
# set the following to True.
fold_rawdata          = True

# Tunable parameters for searching and folding
# (you probably don't need to tune any of them)
datatype_flag           = "-psrfits" # PRESTO flag to determine data type
rfifind_chunk_time      = 2**15 * 0.000064  # ~2.1 sec for dt = 64us
singlepulse_threshold   = 5.0  # threshold SNR for candidate determination
singlepulse_plot_SNR    = 6.0  # threshold SNR for singlepulse plot
singlepulse_maxwidth    = 0.1  # max pulse width in seconds
to_prepfold_sigma       = 6.0  # incoherent sum significance to fold candidates
max_cands_to_fold       = 50   # Never fold more than this many candidates
numhits_to_fold         = 2    # Number of DMs with a detection needed to fold
low_DM_cutoff           = 2.0  # Lowest DM to consider as a "real" pulsar
lo_accel_numharm        = 16   # max harmonics
lo_accel_sigma          = 2.0  # threshold gaussian significance
lo_accel_zmax           = 0    # bins
lo_accel_flo            = 2.0  # Hz
hi_accel_numharm        = 8    # max harmonics
hi_accel_sigma          = 3.0  # threshold gaussian significance
hi_accel_zmax           = 50   # bins
hi_accel_flo            = 1.0  # Hz
low_T_to_search         = 20.0 # sec

# The following is the path where the temporary working directory 
# should be created. This could be /dev/shm, or simply another 
# directory on the worker node.
base_tmp_dir = "/dev/shm/"

# DDplan configurations
# The following configurations are for calculating dedispersion plans 
# on demand. Currently dedispersion plans for WAPP and Mock data 
# are hardcoded.
# lodm        = 0      # pc cm-3
# hidm        = 1000   # pc cm-3
# resolution  = 0.1    # ms
# if use_subbands:
#     numsub  = 96     # subbands
# else:
#     numsub  = 0      # Defaults to number of channels

# Sifting specific parameters (don't touch without good reason!)
sifting_sigma_threshold = to_prepfold_sigma-1.0  
                                 # incoherent power threshold (sigma)
sifting_c_pow_threshold = 100.0  # coherent power threshold
sifting_r_err           = 1.1    # Fourier bin tolerence for candidate equivalence
sifting_short_period    = 0.0005 # Shortest period candidates to consider (s)
sifting_long_period     = 15.0   # Longest period candidates to consider (s)
sifting_harm_pow_cutoff = 8.0    # Power required in at least one harmonic

import searching_check
searching_check.searching.populate_configs(locals())
searching_check.searching.check_sanity()
