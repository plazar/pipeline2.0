#!/usr/bin/env python
import glob
import os
import os.path
import shutil
import socket
import struct
import sys
import time
import subprocess
import warnings
import re
import types
import tarfile
import tempfile

import numpy as np
import scipy
import psr_utils
import presto
import prepfold

import matplotlib
matplotlib.use('agg') #Use AGG (png) backend to plot
import matplotlib.pyplot as plt
import mysifting as sifting # Temporarily until 'sifting.py' 
                            # in PRESTO is updated

import datafile
import config.searching
import config.processing

# Sifting specific parameters (don't touch without good reason!)
# incoherent power threshold (sigma)
sifting.sigma_threshold = config.searching.sifting_sigma_threshold 
# coherent power threshold
sifting.c_pow_threshold = config.searching.sifting_c_pow_threshold 
# Fourier bin tolerence for candidate equivalence
sifting.r_err           = config.searching.sifting_r_err    
# Shortest period candidates to consider (s)
sifting.short_period    = config.searching.sifting_short_period 
# Longest period candidates to consider (s)
sifting.long_period     = config.searching.sifting_long_period   
# Power required in at least one harmonic
sifting.harm_pow_cutoff = config.searching.sifting_harm_pow_cutoff

debug = 0

def get_baryv(ra, dec, mjd, T, obs="AO"):
   """
   get_baryv(ra, dec, mjd, T):
     Determine the average barycentric velocity towards 'ra', 'dec'
       during an observation from 'obs'.  The RA and DEC are in the
       standard string format (i.e. 'hh:mm:ss.ssss' and 
       'dd:mm:ss.ssss'). 'T' is in sec and 'mjd' is (of course) in MJD.
   """
   tts = psr_utils.span(mjd, mjd+T/86400.0, 100)
   nn = len(tts)
   bts = np.zeros(nn, dtype=np.float64)
   vel = np.zeros(nn, dtype=np.float64)
   presto.barycenter(tts, bts, vel, nn, ra, dec, obs, "DE200")
   avgvel = np.add.reduce(vel)/nn
   return avgvel

def find_masked_fraction(obs):
    """
    find_masked_fraction(obs):
        Parse the output file from an rfifind run and return the
            fraction of the data that was suggested to be masked.
    """
    rfifind_out = obs.basefilenm + "_rfifind.out"
    for line in open(rfifind_out):
        if "Number of  bad   intervals" in line:
            return float(line.split("(")[1].split("%")[0])/100.0
    # If there is a problem reading the file, return 100%
    return 100.0

def get_all_subdms(ddplans):
    """
    get_all_subdms(ddplans):
        Return a sorted array of the subdms from the list of ddplans.
    """
    subdmlist = []
    for ddplan in ddplans:
        subdmlist += [float(x) for x in ddplan.subdmlist]
    subdmlist.sort()
    subdmlist = np.asarray(subdmlist)
    return subdmlist


def find_closest_subbands(obs, subdms, DM):
    """
    find_closest_subbands(obs, subdms, DM):
        Return the basename of the closest set of subbands to DM
        given an obs_info class and a sorted array of the subdms.
    """
    subdm = subdms[np.fabs(subdms - DM).argmin()]
    return "obs.tempdir/%s_DM%.2f.sub[0-6]*"%(obs.basefilenm, subdm)


def timed_execute(cmd, stdout=None, stderr=sys.stderr): 
    """
    timed_execute(cmd, stdout=None, stderr=sys.stderr):
        Execute the command 'cmd' after logging the command
            to STDOUT.  Return the wall-clock amount of time
            the command took to execute.

            Output standard output to 'stdout' and standard
            error to 'stderr'. Both are strings containing filenames.
            If values are None, the out/err streams are not recorded.
            By default stdout is None and stderr is combined with stdout.
    """
    # Log command to stdout
    sys.stdout.write("\n'"+cmd+"'\n")
    sys.stdout.flush()

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True
    
    # Run (and time) the command. Check for errors.
    start = time.time()
    retcode = subprocess.call(cmd, shell=True, stdout=stdout, stderr=stderr)
    if retcode < 0:
        raise PrestoError("Execution of command (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise PrestoError("Execution of command (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass
    end = time.time()
    
    # Close file objects, if any
    if stdoutfile:
        stdout.close()
    if stderrfile:
        stderr.close()
    return end - start


def get_folding_command(cand, obs):
    """
    get_folding_command(cand, obs):
        Return a command for prepfold for folding the subbands using
            an obs_info instance, and a candidate instance that 
            describes the observations and searches.
    """
    # Folding rules are based on the facts that we want:
    #   1.  Between 24 and 200 bins in the profiles
    #   2.  For most candidates, we want to search length = 101 p/pd/DM cubes
    #       (The side of the cube is always 2*M*N+1 where M is the "factor",
    #       either -npfact (for p and pd) or -ndmfact, and N is the number of bins
    #       in the profile).  A search of 101^3 points is pretty fast.
    #   3.  For slow pulsars (where N=100 or 200), since we'll have to search
    #       many points, we'll use fewer intervals in time (-npart 30)
    #   4.  For the slowest pulsars, in order to avoid RFI, we'll
    #       not search in period-derivative.
    zmax = cand.filename.split("_")[-1]
    outfilenm = obs.basefilenm+"_DM%s_Z%s"%(cand.DMstr, zmax)

    # Note:  the following calculations should probably only be done once,
    #        but in general, these calculation are effectively instantaneous
    #        compared to the folding itself
    if config.searching.fold_rawdata:
        # Fold raw data
        foldfiles = obs.filenmstr
        mask = "-mask %s" % (obs.basefilenm + "_rfifind.mask")
    else:
        if config.searching.use_subbands:
            # Fold the subbands
            subdms = get_all_subdms(obs.ddplans)
            subfiles = find_closest_subbands(obs, subdms, cand.DM)
            foldfiles = subfiles
            mask = ""
        else:  # Folding the downsampled PSRFITS files instead
            #
            # TODO: Apply mask!?
            #
            mask = ""
            hidms = [x.lodm for x in obs.ddplans[1:]] + [2000]
            dfacts = [x.downsamp for x in obs.ddplans]
            for hidm, dfact in zip(hidms, dfacts):
                if cand.DM < hidm:
                    downsamp = dfact
                    break
            if downsamp==1:
                foldfiles = obs.filenmstr
            else:
                dsfiles = [] 
                for f in obs.filenames:
                    fbase = f.rstrip(".fits")
                    dsfiles.append(fbase+"_DS%d.fits"%downsamp)
                foldfiles = ' '.join(dsfiles)
    p = 1.0 / cand.f
    if p < 0.002:
        Mp, Mdm, N = 2, 2, 24
        npart = 50
        otheropts = "-ndmfact 3"
    elif p < 0.05:
        Mp, Mdm, N = 2, 1, 50
        npart = 40
        otheropts = "-pstep 1 -pdstep 2 -dmstep 3"
    elif p < 0.5:
        Mp, Mdm, N = 1, 1, 100
        npart = 30
        otheropts = "-pstep 1 -pdstep 2 -dmstep 1 -nodmsearch"
    else:
        Mp, Mdm, N = 1, 1, 200
        npart = 30
        otheropts = "-nopdsearch -pstep 1 -pdstep 2 -dmstep 1 -nodmsearch"

    #otheropts += " -fixchi" if config.searching.use_fixchi else ""

    # If prepfold is instructed to use more subbands than there are rows in the PSRFITS file
    # it doesn't use any data when folding since the amount of data for each part is
    # shorter than the PSRFITS row. However, PRESTO doesn't break up rows.
    # Set npart to the number of rows in the PSRFITS file.
    if npart > obs.numrows:
        npart = obs.numrows

    # Get number of subbands to use
    if obs.backend.lower() == 'pdev':
        nsub = 96
    else:
        nsub = 64
    return "prepfold -noxwin -accelcand %d -accelfile %s.cand -dm %.2f -o %s " \
                "-nsub %d -npart %d %s -n %d -npfact %d -ndmfact %d %s %s" % \
           (cand.candnum, cand.filename, cand.DM, outfilenm, nsub,
            npart, otheropts, N, Mp, Mdm, mask, foldfiles)


class obs_info:
    """
    class obs_info(filenms, resultsdir)
        A class describing the observation and the analysis.
    """
    def __init__(self, filenms, resultsdir, zerodm):
        
        self.zerodm = zerodm

        self.filenms = filenms
        self.filenmstr = ' '.join(self.filenms)
        self.basefilenm = os.path.split(filenms[0])[1].rstrip(".fits")
        
        # Where to dump all the results.
        # Put zerodm results in a separate folder so they don't overwrite
        # the non-zerodm results
        if self.zerodm:
            self.outputdir = os.path.join(resultsdir,'zerodm')
            self.basefilenm = self.basefilenm + '_zerodm'
        else:
            self.outputdir = resultsdir

        # Read info from PSRFITS file
        data = datafile.autogen_dataobj(self.filenms)
        # Correct positions in data file headers
        data.update_positions()
        
        spec_info = data.specinfo
        self.backend = spec_info.backend
        self.MJD = spec_info.start_MJD[0]
        self.ra_string = spec_info.ra_str
        self.dec_string = spec_info.dec_str
        self.orig_N = spec_info.N
        self.dt = spec_info.dt # in sec
        self.BW = spec_info.BW
        self.orig_T = spec_info.T
        # Downsampling is catered to the number of samples per row.
        # self.N = psr_utils.choose_N(self.orig_N)
        self.N = self.orig_N
        self.T = self.N * self.dt
        self.nchan = spec_info.num_channels
        self.samp_per_row = spec_info.spectra_per_subint
        self.fctr = spec_info.fctr
        self.numrows = np.sum(spec_info.num_subint) 
       
        # Determine the average barycentric velocity of the observation
        self.baryv = get_baryv(self.ra_string, self.dec_string,
                               self.MJD, self.T, obs="AO")
        # Figure out which host we are processing on
        self.hostname = socket.gethostname()
        # The fraction of the data recommended to be masked by rfifind
        self.masked_fraction = 0.0
        # The number of candidates folded
        self.num_cands_folded = 0
        # Initialize our timers
        self.rfifind_time = 0.0
        self.downsample_time = 0.0
        self.subbanding_time = 0.0
        self.dedispersing_time = 0.0
        self.FFT_time = 0.0
        self.lo_accelsearch_time = 0.0
        self.hi_accelsearch_time = 0.0
        self.singlepulse_time = 0.0
        self.sp_grouping_time = 0.0
        self.sifting_time = 0.0
        self.folding_time = 0.0
        self.zerodm_time = 0.0
        self.total_time = 0.0
        # Inialize some candidate counters
        self.num_sifted_cands = 0
        self.num_folded_cands = 0
        self.num_single_cands = 0
        # Set dedispersion plan
        self.set_DDplan()

    def set_DDplan(self):
        """Set the dedispersion plan.

            The dedispersion plans are hardcoded and
            depend on the backend data were recorded with.
        """
        # Generate dedispersion plan
        self.ddplans = []

        # The following code will run the dedispersion planner on demand.
        # Instead, dedispersion plans for WAPP and Mock data are hardcoded.
        #
        # import DDplan2b
        # obs = DDplan2b.Observation(self.dt, self.fctr, self.BW, self.nchan, \
        #                             self.samp_per_row)
        # plan = obs.gen_ddplan(config.searching.lodm, config.searching.hidm, \
        #                       config.searching.numsub, config.searching.resolution)
        # plan.plot(fn=os.path.join(self.outputdir, self.basefilenm+"_ddplan.ps"))
        # print plan
        # for ddstep in plan.DDsteps:
        #     self.ddplans.append(dedisp_plan(ddstep.loDM, ddstep.dDM, ddstep.DMs_per_prepsub, \
        #                    ddstep.numprepsub, ddstep.numsub, ddstep.downsamp))

        if self.backend.lower() == 'pdev':
            # The values here are:       lodm dmstep dms/call #calls #subbands downsamp
            self.ddplans.append(dedisp_plan(   0.0,  0.1,    76,     28,     96,        1 ))
            self.ddplans.append(dedisp_plan( 212.8,  0.3,    64,     12,     96,        2 ))
            self.ddplans.append(dedisp_plan( 443.2,  0.3,    76,      4,     96,        3 ))
            self.ddplans.append(dedisp_plan( 534.4,  0.5,    76,      9,     96,        5 ))
            self.ddplans.append(dedisp_plan( 876.4,  0.5,    76,      3,     96,        6 ))
            self.ddplans.append(dedisp_plan( 990.4,  1.0,    76,     11,     96,       10 ))
            self.ddplans.append(dedisp_plan(1826.4,  2.0,    72,     10,     96,       15 ))
            self.ddplans.append(dedisp_plan(3266.4,  3.0,    76,      8,     96,       30 ))
        elif self.backend.lower() == 'wapp':
            # The values here are:       lodm dmstep dms/call #calls #subbands downsamp
            self.ddplans.append(dedisp_plan(   0.0,  0.3,    76,      9,     96,        1 ))
            self.ddplans.append(dedisp_plan( 205.2,  2.0,    76,      5,     96,        5 ))
            self.ddplans.append(dedisp_plan( 965.2, 10.0,    76,      1,     96,       25 ))
        else:
            raise ValueError("No dediserpsion plan for unknown backend (%s)!" % self.backend)
        

    def write_report(self, filenm):
        report_file = open(filenm, "w")
        report_file.write("---------------------------------------------------------\n")
        report_file.write("Data (%s) were processed on %s\n" % \
                                (', '.join(self.filenms), self.hostname))
        report_file.write("Ending UTC time:  %s\n"%(time.asctime(time.gmtime())))
        report_file.write("Total wall time:  %.1f s (%.2f hrs)\n"%\
                          (self.total_time, self.total_time/3600.0))
        report_file.write("Fraction of data masked:  %.2f%%\n"%\
                          (self.masked_fraction*100.0))
        report_file.write("Number of candidates folded: %d\n"%\
                          self.num_cands_folded)
        report_file.write("---------------------------------------------------------\n")
        report_file.write("          rfifind time = %7.1f sec (%5.2f%%)\n"%\
                          (self.rfifind_time, self.rfifind_time/self.total_time*100.0))
        if config.searching.use_subbands:
            report_file.write("       subbanding time = %7.1f sec (%5.2f%%)\n"%\
                              (self.subbanding_time, self.subbanding_time/self.total_time*100.0))
        else:
            report_file.write("     downsampling time = %7.1f sec (%5.2f%%)\n"%\
                              (self.downsample_time, self.downsample_time/self.total_time*100.0))
        report_file.write("     dedispersing time = %7.1f sec (%5.2f%%)\n"%\
                          (self.dedispersing_time, self.dedispersing_time/self.total_time*100.0))
        report_file.write("     single-pulse time = %7.1f sec (%5.2f%%)\n"%\
                          (self.singlepulse_time, self.singlepulse_time/self.total_time*100.0))
        if config.searching.sp_grouping:
            report_file.write("      SP grouping time = %7.1f sec (%5.2f%%)\n"%\
                              (self.sp_grouping_time, self.sp_grouping_time/self.total_time*100.0))
        report_file.write("              FFT time = %7.1f sec (%5.2f%%)\n"%\
                          (self.FFT_time, self.FFT_time/self.total_time*100.0))
        report_file.write("   lo-accelsearch time = %7.1f sec (%5.2f%%)\n"%\
                          (self.lo_accelsearch_time, self.lo_accelsearch_time/self.total_time*100.0))
        report_file.write("   hi-accelsearch time = %7.1f sec (%5.2f%%)\n"%\
                          (self.hi_accelsearch_time, self.hi_accelsearch_time/self.total_time*100.0))
        report_file.write("          sifting time = %7.1f sec (%5.2f%%)\n"%\
                          (self.sifting_time, self.sifting_time/self.total_time*100.0))
        report_file.write("          folding time = %7.1f sec (%5.2f%%)\n"%\
                          (self.folding_time, self.folding_time/self.total_time*100.0))
        if self.zerodm_time:
            report_file.write("       zerodm job time = %7.1f sec (%5.2f%%)\n"%\
                              (self.zerodm_time, self.zerodm_time/self.total_time*100.0))
        report_file.write("---------------------------------------------------------\n")
        report_file.close()

class dedisp_plan:
    """
    class dedisp_plan(lodm, dmstep, dmsperpass, numpasses, numsub, downsamp)
        A class describing a de-dispersion plan for prepsubband in detail.
    """
    def __init__(self, lodm, dmstep, dmsperpass, numpasses, numsub, downsamp):
        self.lodm = float(lodm)
        self.dmstep = float(dmstep)
        self.dmsperpass = int(dmsperpass)
        self.numpasses = int(numpasses)
        self.numsub = int(numsub)
        self.downsamp = int(downsamp)
        # Downsample less for the subbands so that folding
        # candidates is more acurate
        #
        # Turning this off because downsampling factors are not necessarily
        # powers of 2 any more! Also, are we folding from raw data now?
        # -- PL Nov. 26, 2010
        #
        self.sub_downsamp = self.downsamp
        self.dd_downsamp = 1
        # self.sub_downsamp = self.downsamp / 2
        # if self.sub_downsamp==0: self.sub_downsamp = 1
        # The total downsampling is:
        #   self.downsamp = self.sub_downsamp * self.dd_downsamp

        # if self.downsamp==1: self.dd_downsamp = 1
        # else: self.dd_downsamp = 2
        self.sub_dmstep = self.dmsperpass * self.dmstep
        self.dmlist = []  # These are strings for comparison with filenames
        self.subdmlist = []
        for ii in range(self.numpasses):
            self.subdmlist.append("%.2f"%(self.lodm + (ii+0.5)*self.sub_dmstep))
            lodm = self.lodm + ii * self.sub_dmstep
            dmlist = ["%.2f"%dm for dm in \
                      np.arange(self.dmsperpass)*self.dmstep + lodm]
            self.dmlist.append(dmlist)


def main(filenms, workdir, resultsdir):

    # Change to the specified working directory
    os.chdir(workdir)

    job = set_up_job(filenms, workdir, resultsdir)
    
    print "\nBeginning PALFA search of %s" % (', '.join(job.filenms))
    print "UTC time is:  %s"%(time.asctime(time.gmtime()))
    
    try:
        search_job(job)
    except:
        print "***********************ERRORS!************************"
        print "  Search has been aborted due to errors encountered."
        print "  See error output for more information."
        print "******************************************************"
        raise
    finally:
        clean_up(job)

    # Do search with zerodming
    if config.searching.use_zerodm:
        zerodm_job = set_up_job(filenms, workdir, resultsdir, zerodm=True) 

        # copy zaplist from non-zerodm job to zerodm job workdir
        zaplist = glob.glob(os.path.join(job.outputdir,'*.zaplist'))[0]
        shutil.copy(zaplist,zerodm_job.workdir)

        # copy raw data file to zerodm workdir
        for fn in filenms:
            shutil.copy(fn,zerodm_job.workdir)

        os.chdir(zerodm_job.workdir)

        try:
            search_job(zerodm_job)
        except:
            print "***********************ERRORS!************************"
            print "  Search has been aborted due to errors encountered."
            print "  See error output for more information."
            print "******************************************************"
            raise
        finally:
            clean_up(zerodm_job)
            # Write the job report for zerodm job
            zerodm_job.total_time = time.time() - zerodm_job.total_time
            zerodm_job.write_report(os.path.join(zerodm_job.outputdir, zerodm_job.basefilenm+".report"))
            job.zerodm_time = zerodm_job.total_time

    # Write the job report
    job.total_time = time.time() - job.total_time
    job.write_report(os.path.join(job.outputdir, job.basefilenm+".report"))

    # And finish up
    print "\nFinished"
    print "UTC time is:  %s"%(time.asctime(time.gmtime()))


    
def set_up_job(filenms, workdir, resultsdir,zerodm=False):
    """Change to the working directory and set it up.
        Create a obs_info instance, set it up and return it.
    """
    # Get information on the observation and the job
    job = obs_info(filenms, resultsdir, zerodm)
    if job.T < config.searching.low_T_to_search:
        raise PrestoError("The observation is too short to search. " \
                            "(%.2f s < %.2f s)" % \
                            (job.T, config.searching.low_T_to_search))
    job.total_time = time.time()
    
    # Make sure the output directory (and parent directories) exist
    try:
        os.makedirs(job.outputdir)
    except: pass

    if zerodm:
        zerodm_workdir = os.path.join(workdir,'zerodm')
        os.mkdir(zerodm_workdir)
        job.workdir = zerodm_workdir
    else:
        job.workdir = workdir

    # Create a directory to hold all the subbands
    if config.processing.use_pbs_subdir:
        pbs_job_id = os.getenv("PBS_JOBID")
        base_tmp_dir = os.path.join(config.processing.base_tmp_dir, \
                                    pbs_job_id) 
    else:
        base_tmp_dir = config.processing.base_tmp_dir

    job.tempdir = tempfile.mkdtemp(suffix="_tmp", prefix=job.basefilenm, \
                                   dir=base_tmp_dir)
    
    #####
    # Print some info useful for debugging
    print "Initial contents of workdir (%s): " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Initial contents of resultsdir (%s): " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    print "Initial contents of job.tempdir (%s): " % job.tempdir
    for fn in os.listdir(job.tempdir):
        print "    %s" % fn
    sys.stdout.flush()
    #####

    return job


def search_job(job):
    """Search the observation defined in the obs_info
        instance 'job'.
    """

    zerodm_flag = '-zerodm' if job.zerodm else ''

    # Use whatever .zaplist is found in the current directory
    zaplist = glob.glob("*.zaplist")[0]
    print "Using %s as zaplist" % zaplist
    if config.searching.use_subbands and config.searching.fold_rawdata:
        # make a directory to keep subbands so they can be used to fold later
        try:
            os.makedirs(os.path.join(job.workdir, 'subbands'))
        except: pass

    # rfifind the data file
    cmd = "rfifind %s %s -time %.17g -o %s %s" % \
          (config.searching.datatype_flag, zerodm_flag, config.searching.rfifind_chunk_time, 
           job.basefilenm, job.filenmstr)
    job.rfifind_time += timed_execute(cmd, stdout="%s_rfifind.out" % job.basefilenm)
    maskfilenm = job.basefilenm + "_rfifind.mask"
    # Find the fraction that was suggested to be masked
    # Note:  Should we stop processing if the fraction is
    #        above some large value?  Maybe 30%?
    job.masked_fraction = find_masked_fraction(job)
    
    # Iterate over the stages of the overall de-dispersion plan
    dmstrs = []
    for ddplan in job.ddplans:

        # Iterate over the individual passes through the data file
        for passnum in range(ddplan.numpasses):
            subbasenm = "%s_DM%s"%(job.basefilenm, ddplan.subdmlist[passnum])

            if config.searching.use_subbands:
                try:
                    os.makedirs(os.path.join(job.tempdir, 'subbands'))
                except: pass
    
                # Create a set of subbands
                cmd = "prepsubband %s %s -sub -subdm %s -downsamp %d -nsub %d -mask %s " \
                        "-o %s/subbands/%s %s" % \
                        (config.searching.datatype_flag, zerodm_flag, ddplan.subdmlist[passnum], 
                        ddplan.sub_downsamp, ddplan.numsub, maskfilenm, job.tempdir, 
                        job.basefilenm, job.filenmstr)
                job.subbanding_time += timed_execute(cmd, stdout="%s.subout" % subbasenm)
            
                # Now de-disperse using the subbands
                cmd = "prepsubband -lodm %.2f -dmstep %.2f -numdms %d -downsamp %d " \
                        "-nsub %d -numout %d -o %s/%s %s/subbands/%s.sub[0-9]*" % \
                        (ddplan.lodm+passnum*ddplan.sub_dmstep, ddplan.dmstep,
                        ddplan.dmsperpass, ddplan.dd_downsamp, ddplan.numsub,
                        psr_utils.choose_N(job.orig_N/ddplan.downsamp),
                        job.tempdir, job.basefilenm, job.tempdir, subbasenm)
                job.dedispersing_time += timed_execute(cmd, stdout="%s.prepout" % subbasenm)
            
            else:  # Not using subbands
                cmd = "prepsubband %s -mask %s -lodm %.2f -dmstep %.2f -numdms %d -downsamp %d " \
                        "-numout %d -nsub %d -o %s/%s %s"%\
                        (zerodm_flag, maskfilenm, ddplan.lodm+passnum*ddplan.sub_dmstep, 
                        ddplan.dmstep, ddplan.dmsperpass, ddplan.dd_downsamp*ddplan.sub_downsamp, 
                        psr_utils.choose_N(job.orig_N/ddplan.downsamp), ddplan.numsub, 
                        job.tempdir, job.basefilenm, job.filenmstr)
                job.dedispersing_time += timed_execute(cmd)
            
            # Iterate over all the new DMs
            basenms_forpass = []
            for dmstr in ddplan.dmlist[passnum]:
                dmstrs.append(dmstr)
                basenm = os.path.join(job.tempdir, job.basefilenm+"_DM"+dmstr)
                basenms_forpass.append(basenm)

                datnm = basenm+".dat"
                fftnm = basenm+".fft"
                infnm = basenm+".inf"


                # FFT, zap, and de-redden
                cmd = "realfft %s"%datnm
                job.FFT_time += timed_execute(cmd)
                cmd = "zapbirds -zap -zapfile %s -baryv %.6g %s"%\
                      (zaplist, job.baryv, fftnm)
                job.FFT_time += timed_execute(cmd)
                cmd = "rednoise %s"%fftnm
                job.FFT_time += timed_execute(cmd)
                try:
                    os.rename(basenm+"_red.fft", fftnm)
                except: pass
                
                # Do the low-acceleration search
                cmd = "accelsearch -harmpolish -numharm %d -sigma %f " \
                        "-zmax %d -flo %f %s"%\
                        (config.searching.lo_accel_numharm, \
                         config.searching.lo_accel_sigma, \
                         config.searching.lo_accel_zmax, \
                         config.searching.lo_accel_flo, fftnm)
                job.lo_accelsearch_time += timed_execute(cmd)
                try:
                    os.remove(basenm+"_ACCEL_%d.txtcand" % config.searching.lo_accel_zmax)
                except: pass
                try:  # This prevents errors if there are no cand files to copy
                    shutil.move(basenm+"_ACCEL_%d.cand" % config.searching.lo_accel_zmax, \
                                    job.workdir)
                    shutil.move(basenm+"_ACCEL_%d" % config.searching.lo_accel_zmax, \
                                    job.workdir)
                except: pass
        
                # Do the high-acceleration search (only for non-zerodm case)
                if not job.zerodm:
                    cmd = "accelsearch -harmpolish -numharm %d -sigma %f " \
                            "-zmax %d -flo %f %s"%\
                            (config.searching.hi_accel_numharm, \
                             config.searching.hi_accel_sigma, \
                             config.searching.hi_accel_zmax, \
                             config.searching.hi_accel_flo, fftnm)
                    job.hi_accelsearch_time += timed_execute(cmd)
                    try:
                        os.remove(basenm+"_ACCEL_%d.txtcand" % config.searching.hi_accel_zmax)
                    except: pass
                    try:  # This prevents errors if there are no cand files to copy
                        shutil.move(basenm+"_ACCEL_%d.cand" % config.searching.hi_accel_zmax, \
                                        job.workdir)
                        shutil.move(basenm+"_ACCEL_%d" % config.searching.hi_accel_zmax, \
                                        job.workdir)
                    except: pass

                # Remove the .fft files
                try:
                    os.remove(fftnm)
                except: pass

            # Do the single-pulse search
            dats_str = '.dat '.join(basenms_forpass) + '.dat'
            if job.zerodm:
                cmd = "single_pulse_search.py -b -p -m %f -t %f %s"%\
                      (config.searching.singlepulse_maxwidth, \
                       config.searching.singlepulse_threshold, dats_str)
            else:
                cmd = "single_pulse_search.py -p -m %f -t %f %s"%\
                      (config.searching.singlepulse_maxwidth, \
                       config.searching.singlepulse_threshold, dats_str)
            job.singlepulse_time += timed_execute(cmd)

            # Move .singlepulse and .inf files and delete .dat files
            for basenm in basenms_forpass:
                try:
                    shutil.move(basenm+".singlepulse", job.workdir)
                    shutil.move(basenm+".inf", job.workdir)
                    os.remove(basenm+".dat")
                except: pass
            
            if config.searching.use_subbands:
                if config.searching.fold_rawdata:
                    # Subband files are no longer needed
                    shutil.rmtree(os.path.join(job.tempdir, 'subbands'))
                else:
                    # Move subbands to workdir
                    for sub in glob.glob(os.path.join(job.tempdir, 'subbands', "*")):
                        shutil.move(sub, os.path.join(job.workdir, 'subbands'))

    # Make the single-pulse plots
    basedmb = job.basefilenm+"_DM"
    basedme = ".singlepulse "
    # The following will make plots for DM ranges:
    #    0-110, 100-310, 300-1000+
    dmglobs = [basedmb+"[0-9].[0-9][0-9]"+basedme +
               basedmb+"[0-9][0-9].[0-9][0-9]"+basedme +
               basedmb+"10[0-9].[0-9][0-9]"+basedme,
               basedmb+"[12][0-9][0-9].[0-9][0-9]"+basedme +
               basedmb+"30[0-9].[0-9][0-9]"+basedme,
               basedmb+"[3-9][0-9][0-9].[0-9][0-9]"+basedme +
               basedmb+"1[0-9][0-9][0-9].[0-9][0-9]"+basedme,
               basedmb+"[1-5][0-9][0-9][0-9].[0-9][0-9]"+basedme]
    dmrangestrs = ["0-110", "100-310", "300-1000+","1000-5000+"]
    psname = job.basefilenm+"_singlepulse.ps"

    for dmglob, dmrangestr in zip(dmglobs, dmrangestrs):
        dmfiles = []
        for dmg in dmglob.split():
            dmfiles += glob.glob(dmg.strip())
        # Check that there are matching files and they are not all empty
        if dmfiles and sum([os.path.getsize(f) for f in dmfiles]):
            cmd = 'single_pulse_search.py -t %f -g "%s"' % \
                (config.searching.singlepulse_plot_SNR, dmglob)
            job.singlepulse_time += timed_execute(cmd)
            os.rename(psname,
                    job.basefilenm+"_DMs%s_singlepulse.ps" % dmrangestr)

    # Do singlepulse grouping analysis (Chen Karako's code)
    if config.searching.sp_grouping and job.masked_fraction < 0.2:
        job.sp_grouping_time = time.time()
        import analyse_sp_palfa
        analyse_sp_palfa.main()
        job.sp_grouping_time = time.time() - job.sp_grouping_time

    # Sift through the candidates to choose the best to fold
    job.sifting_time = time.time()

    lo_accel_cands = sifting.read_candidates(glob.glob("*ACCEL_%d" % config.searching.lo_accel_zmax))
    if len(lo_accel_cands):
        lo_accel_cands = sifting.remove_duplicate_candidates(lo_accel_cands)
    if len(lo_accel_cands):
        lo_accel_cands = sifting.remove_DM_problems(lo_accel_cands, config.searching.numhits_to_fold,
                                                    dmstrs, config.searching.low_DM_cutoff)

    hi_accel_cands = sifting.read_candidates(glob.glob("*ACCEL_%d" % config.searching.hi_accel_zmax))
    if len(hi_accel_cands):
        hi_accel_cands = sifting.remove_duplicate_candidates(hi_accel_cands)
    if len(hi_accel_cands):
        hi_accel_cands = sifting.remove_DM_problems(hi_accel_cands, config.searching.numhits_to_fold,
                                                    dmstrs, config.searching.low_DM_cutoff)

    all_accel_cands = lo_accel_cands + hi_accel_cands
    if len(all_accel_cands):
        all_accel_cands = sifting.remove_harmonics(all_accel_cands)
        # Note:  the candidates will be sorted in _sigma_ order, not _SNR_!
        all_accel_cands.sort(sifting.cmp_sigma)
        print "Sending candlist to stdout before writing to file"
        sifting.write_candlist(all_accel_cands)
        sys.stdout.flush()
        sifting.write_candlist(all_accel_cands, job.basefilenm+".accelcands")
        # Make sifting summary plots
        all_accel_cands.plot_goodbad()
        plt.title("%s Rejected Cands" % job.basefilenm)
        plt.savefig(job.basefilenm+".accelcands.rejects.png")
        all_accel_cands.plot_summary()
        plt.title("%s Periodicity Summary" % job.basefilenm)
        plt.savefig(job.basefilenm+".accelcands.summary.png")
        
        # Write out sifting candidate summary
        all_accel_cands.print_cand_summary(job.basefilenm+".accelcands.summary")
        # Write out sifting comprehensive report of bad candidates
        all_accel_cands.write_cand_report(job.basefilenm+".accelcands.report")
        timed_execute("gzip --best %s" % job.basefilenm+".accelcands.report")

        # Moving of results to resultsdir now happens in clean_up(...)
        # shutil.copy(job.basefilenm+".accelcands", job.outputdir)

    job.sifting_time = time.time() - job.sifting_time

    #####
    # Print some info useful for debugging
    print "Contents of workdir (%s) before folding: " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Contents of resultsdir (%s) before folding: " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    print "Contents of job.tempdir (%s) before folding: " % job.tempdir
    for fn in os.listdir(job.tempdir):
        print "    %s" % fn
    sys.stdout.flush()
    #####

    # Fold the best candidates
    cands_folded = 0
    for cand in all_accel_cands:
        print "At cand %s" % str(cand)
        if cands_folded == config.searching.max_cands_to_fold:
            break
        if cand.sigma >= config.searching.to_prepfold_sigma:
            print "...folding"
            job.folding_time += timed_execute(get_folding_command(cand, job))
            cands_folded += 1
    job.num_cands_folded = cands_folded
    
    # Set up theano compile dir (UBC_AI rating uses theano)
    theano_compiledir = os.path.join(job.tempdir,'theano_compile')
    os.mkdir(theano_compiledir)
    os.putenv("THEANO_FLAGS","compiledir=%s" % theano_compiledir) 

    # Rate candidates
    timed_execute("rate_pfds.py --redirect-warnings --include-all -x prepfold_sigma *.pfd")
    sys.stdout.flush()

    # Calculate some candidate attributes from pfds
    attrib_file = open('candidate_attributes.txt','w')
    for pfdfn in glob.glob("*.pfd"):
        attribs = {}
        pfd = prepfold.pfd(pfdfn)
        red_chi2 = pfd.bestprof.chi_sqr
        dof = pfd.proflen - 1
        attribs['prepfold_sigma'] = \
                -scipy.stats.norm.ppf(scipy.stats.chi2.sf(red_chi2*dof, dof))
        
        if config.searching.use_fixchi:
            # Remake prepfold plot with rescaled chi-sq
            cmd = "show_pfd -noxwin -fixchi %s" % pfdfn
            timed_execute(cmd) 

            # Get prepfold sigma from the rescaled bestprof
            pfd = prepfold.pfd(pfdfn)
            red_chi2 = pfd.bestprof.chi_sqr
            attribs['rescaled_prepfold_sigma'] = \
                    -scipy.stats.norm.ppf(scipy.stats.chi2.sf(red_chi2*dof, dof))
        else:
            # Rescale prepfold sigma by estimating the off-signal
            # reduced chi-sq
	    off_red_chi2 = pfd.estimate_offsignal_redchi2()
	    new_red_chi2 = red_chi2 / off_red_chi2
            attribs['rescaled_prepfold_sigma'] = \
                    -scipy.stats.norm.ppf(scipy.stats.chi2.sf(new_red_chi2*dof, dof))

        for key in attribs:
            attrib_file.write("%s\t%s\t%.3f\n" % (pfdfn, key, attribs[key]))
    attrib_file.close()

    # Print some info useful for debugging
    print "Contents of workdir (%s) after folding: " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Contents of resultsdir (%s) after folding: " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    print "Contents of job.tempdir (%s) after folding: " % job.tempdir
    for fn in os.listdir(job.tempdir):
        print "    %s" % fn
    sys.stdout.flush()
    #####
    
    # Now step through the .ps files and convert them to .png and gzip them

    psfiles = glob.glob("*.ps")
    for psfile in psfiles:
        # The '[0]' appeneded to the end of psfile is to convert only the 1st page
        timed_execute("convert -quality 90 %s -background white -trim -rotate 90 +matte %s" % \
                            (psfile+"[0]", psfile[:-3]+".png"))
        timed_execute("gzip "+psfile)
    
    # Print some info useful for debugging
    print "Contents of workdir (%s) after conversion: " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Contents of resultsdir (%s) after conversion: " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    print "Contents of job.tempdir (%s) after conversion: " % job.tempdir
    for fn in os.listdir(job.tempdir):
        print "    %s" % fn
    sys.stdout.flush()
    #####


def clean_up(job):
    """Clean up.
        Tar results, copy them to the results directory.
    """
    # Dump search paramters to file
    paramfn = open("search_params.txt", 'w')
    cfgs = config.searching_check.searching.configs
    for key in cfgs:
        paramfn.write("%-25s = %r\n" % (key, cfgs[key].value))
    paramfn.close()

    # Tar up the results files 
    tar_suffixes = ["_ACCEL_%d.tgz"%config.searching.lo_accel_zmax,
                    "_ACCEL_%d.tgz"%config.searching.hi_accel_zmax,
                    "_ACCEL_%d.cand.tgz"%config.searching.lo_accel_zmax,
                    "_ACCEL_%d.cand.tgz"%config.searching.hi_accel_zmax,
                    "_singlepulse.tgz",
                    "_inf.tgz",
                    "_pfd.tgz",
                    "_bestprof.tgz",
                    "_pfd_rat.tgz"]
    tar_globs = ["*_ACCEL_%d"%config.searching.lo_accel_zmax,
                 "*_ACCEL_%d"%config.searching.hi_accel_zmax,
                 "*_ACCEL_%d.cand"%config.searching.lo_accel_zmax,
                 "*_ACCEL_%d.cand"%config.searching.hi_accel_zmax,
                 "*.singlepulse",
                 "*_DM[0-9]*.inf",
                 "*.pfd",
                 "*.pfd.bestprof",
                 "*.pfd.rat"]
    print "Tarring up results"
    for (tar_suffix, tar_glob) in zip(tar_suffixes, tar_globs):
        print "Opening tarball %s" % (job.basefilenm+tar_suffix)
        print "Using glob %s" % tar_glob
        tf = tarfile.open(job.basefilenm+tar_suffix, "w:gz")
        for infile in glob.glob(tar_glob):
            print "    Adding file %s" % infile
            tf.add(infile)
            os.remove(infile)
        tf.close()
    sys.stdout.flush()
    
    # Copy all the important stuff to the output directory
    resultglobs = ["*rfifind.[bimors]*", "*.tgz", "*.png", \
                    "*.zaplist", "search_params.txt", "*.accelcands*", \
                    "*_merge.out", "candidate_attributes.txt", "groups.txt", \
                    "*_calrows.txt"]
    
    # Print some info useful for debugging
    print "Contents of workdir (%s) before copy: " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Contents of resultsdir (%s) before copy: " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    print "Contents of job.tempdir (%s) before copy: " % job.tempdir
    for fn in os.listdir(job.tempdir):
        print "    %s" % fn
    sys.stdout.flush()
    #####
    
    for resultglob in resultglobs:
            for file in glob.glob(resultglob):
                shutil.move(file, job.outputdir)

    # Remove the tmp directory (in a tmpfs mount)
    try:
        shutil.rmtree(job.tempdir)
    except: pass
  
    #####
    # Print some info useful for debugging
    print "Contents of workdir (%s) after copy: " % job.workdir
    for fn in os.listdir(job.workdir):
        print "    %s" % fn
    print "Contents of resultsdir (%s) after copy: " % job.outputdir
    for fn in os.listdir(job.outputdir):
        print "    %s" % fn
    #print "Contents of job.tempdir (%s) after copy: " % job.tempdir
    #for fn in os.listdir(job.tempdir):
    #    print "    %s" % fn
    sys.stdout.flush()
    #####


class PrestoError(Exception):
    """Error to throw when a PRESTO program returns with 
        a non-zero error code.
    """
    pass


if __name__ == "__main__":
    # Arguments to the search program are
    # sys.argv[3:] = data file names
    # sys.argv[1] = working directory name
    # sys.argv[2] = results directory name
    workdir = sys.argv[1]
    resultsdir = sys.argv[2]
    filenms = sys.argv[3:]
    main(filenms, workdir, resultsdir)
