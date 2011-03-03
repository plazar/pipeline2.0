#!/usr/bin/env python

import optparse
import sys

import numpy as np

# Import matplotlib/pylab and set for non-interactive plots
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import psr_utils

"""
Given observation parameters and a acceptable time resolution
generate a dedispersion plan.

The program generates a good plan for de-dispersing raw data.  
It trades a small amount of sensitivity in order to save computation costs.

(This is a re-write of Scott Ransom's original DDplan.py)

Patrick Lazarus, Sept. 23, 2010
"""

# Define some global constants
# Allowable DM step sizes
ALLOW_DMSTEPS = [0.01, 0.02, 0.03, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0,
                  2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 50.0, 100.0, 200.0, 300.0]
# Maximum downsampling factor
MAX_DOWNFACTOR = 64
# Fudge factor that "softens" the boundary defining
# if two time scales are equal or not
FF = 1.2

# NOTE: (PL)
# 
# Should smearing factor be different from 2.0 if not using powers of 2
# for downsampling? 
# Should smearfact be computed for each ddstep? 
# Can we tollerate larger jumps (ie smaller smearfacts)?
#
SMEARFACT = 2.0

# Set plotting defaults
plt.rc('lines', linewidth=1.5)

class Observation:
    """Observation class.
        Defines relevant observation parameters.
    """
    def __init__(self, dt, fctr, BW, numchan, numsamp=0):
        """Observation object constructor
            Inputs:
                dt: Sample time (in seconds)
                f_ctr: Centre frequency (in MHz)
                BW: Observing bandwidth (in MHz)
                numchan: Number of frequency channels
                numsamp: Number of samples (or samps per row for PSRFITS data)
        """
        # dt in sec, fctr and in MHz
        self.dt = dt
        self.fctr = fctr
        self.BW = BW
        self.numchan = numchan
        self.chanwidth = BW/numchan
        self.numsamp = numsamp
        self.allow_factors = self.get_allow_downfactors()

    def gen_ddplan(self, loDM, hiDM, numsub=0, resolution=0.0, verbose=False):
        """Generate and return a dedispersion plan for
            the observation.
            Returns a DDplan object.
            
            Inputs:
                loDM: Low DM to consider (pc cm-3)
                hiDM: High DM to consider (pc cm-3)
                numsub: Number of subbands to use (Default = Don't subband)
                resolution: Acceptable smearing in ms (Default = Least smearing possible)
                verbose: Print information to screen (Default = False)
        """
        return DDplan(loDM, hiDM, self, numsub, resolution, verbose)

    def get_allow_downfactors(self):
        """Return a list of allowable downsampling factors based on
            self.numsamp. If self.numsamp is 0 use powers of 2.
            All downfactors will be smaller than MAX_DOWNFACTOR.
        """
        if self.numsamp:
            factors = np.arange(1, MAX_DOWNFACTOR+1)
            indices = (self.numsamp % factors)==0
            facts = list(factors[indices])
        else: # No number of samples given, use powers of 2
            facts = list(2**np.arange(0, np.log2(MAX_DOWNFACTOR)+1, dtype='int'))
        return facts


class DDstep:
    """A dedispersion step class.
        A DDstep is one block of a dedispersion plan with constant
        downsampling and DM stepsize.
    """
    def __init__(self, ddplan, downsamp, loDM, dDM, \
                    numDMs=0, numsub=0, smearfact=2.0):
        """DDstep object constructor.

            Inputs:
                ddplan: DDplan object this DDstep is part of.
                downsamp: Downsampling factor.
                loDM: Low DM edge of this DDstep (pc cm-3)
                dDM: DM step size to use (pc cm-3)
                numDMs: Number of DMs. If numDMs=0 compute numDMs
                        based on DM range and spacing.
                        (Default: 0)
                numsub: Number of subbands to use. (Default: no subbanding).
                smearfact: Allowable smearing in a single channel, relative
                        to other smearing contributions (Default: 2.0)
        """
        self.ddplan = ddplan
        self.downsamp = downsamp
        self.loDM = loDM
        self.dDM = dDM
        self.numsub = numsub
        self.BW_smearing = psr_utils.dm_smear(dDM*0.5, self.ddplan.obs.BW, \
                                                self.ddplan.obs.fctr)
        self.numprepsub = 0
        if numsub:
            # Calculate the maximum subband smearing we can handle
            DMs_per_prepsub = 2
            while True:
                next_dsubDM = (DMs_per_prepsub+2)*dDM
                next_ss = psr_utils.dm_smear(next_dsubDM*0.5, \
                                    self.ddplan.obs.BW/numsub, \
                                    self.ddplan.obs.fctr)
                # The 0.8 is a small fudge factor to make sure that the subband
                # smearing is always the smallest contribution
                if next_ss > 0.8*min(self.BW_smearing, \
                                        self.ddplan.obs.dt*self.downsamp):
                    self.dsubDM = DMs_per_prepsub*dDM
                    self.DMs_per_prepsub = DMs_per_prepsub
                    self.sub_smearing = psr_utils.dm_smear(self.dsubDM*0.5,
                                            self.ddplan.obs.BW/self.numsub, \
                                            self.ddplan.obs.fctr)
                    break
                DMs_per_prepsub += 2
        else:
            self.dsubDM = dDM
            self.sub_smearing = 0.0
        
        # Calculate the nominal DM to move to the next step
        cross_DM = self.DM_for_smearfact(smearfact)
        if cross_DM > self.ddplan.hiDM:
            cross_DM = self.ddplan.hiDM
        if numDMs == 0:
            self.numDMs = int(np.ceil((cross_DM-self.loDM)/self.dDM))
            if numsub:
                self.numprepsub = int(np.ceil(self.numDMs*self.dDM / self.dsubDM))
                self.numDMs = self.numprepsub * DMs_per_prepsub
        else:
            self.numDMs = numDMs
        self.hiDM = loDM + self.numDMs*dDM
        self.DMs = np.arange(self.numDMs, dtype='d')*self.dDM + self.loDM
        
        # Calculate a few more smearing values
        self.chan_smear = psr_utils.dm_smear(self.DMs, \
                                            self.ddplan.obs.chanwidth, \
                                            self.ddplan.obs.fctr) 
        self.tot_smear = np.sqrt((self.ddplan.obs.dt)**2.0 + \
                                     (self.ddplan.obs.dt*self.downsamp)**2.0 + \
                                     self.BW_smearing**2.0 + \
                                     self.sub_smearing**2.0 + \
                                     self.chan_smear**2.0)

    def DM_for_smearfact(self, smearfact):
        """
        Return the DM where the smearing in a single channel is a factor smearfact
        larger than all the other smaring causes combined.
        """
        other_smear = np.sqrt((self.ddplan.obs.dt)**2.0 +
                           (self.ddplan.obs.dt*self.downsamp)**2.0 +
                           self.BW_smearing**2.0 +
                           self.sub_smearing**2.0)
        return guess_DMstep(smearfact*other_smear, \
                            self.ddplan.obs.chanwidth, self.ddplan.obs.fctr)

    def __str__(self):
        if (self.numsub):
            return "%9.3f  %9.3f  %6.2f    %4d  %6.2f  %6d  %6d  %6d " % \
                   (self.loDM, self.hiDM, self.dDM, self.downsamp, self.dsubDM,
                    self.numDMs, self.DMs_per_prepsub, self.numprepsub)
        else:
            return "%9.3f  %9.3f  %6.2f    %4d  %6d" % \
                   (self.loDM, self.hiDM, self.dDM, self.downsamp, self.numDMs)


class DDplan:
    """Dedispersion plan class.
        Contains a list of DDsteps.
    """
    def __init__(self, loDM, hiDM, obs, numsub=0, resolution=0.0, \
                    verbose=False):
        """DDplan object constructor.
            
            Inputs:
                loDM: Low DM to consider (pc cm-3)
                hiDM: High DM to consider (pc cm-3)
                obs: Observation object
                numsub: Number of subbands to use (Default = Don't subband)
                resolution: Acceptable smearing in ms (Default = Least smearing possible)
                verbose: Print information to screen (Default = False)
        """
        self.loDM = loDM
        self.hiDM = hiDM
        self.obs = obs
        self.numsub = numsub
        self.req_resolution = resolution*0.001 # In seconds
        self.current_downfact = self.obs.allow_factors[0]
        self.current_dDM = ALLOW_DMSTEPS[0]
        
        self.DDsteps = [] # list of dedispersion steps
    
        # Calculate optimal smearing
        self.calc_min_smearing(verbose=verbose)

        # Calculate initial downsampling
        while (self.obs.dt*self.get_next_downfact()) < self.resolution:
            self.current_downfact = self.get_next_downfact()
        if verbose:
            print "        New dt is %d x %.12g s = %.12g s" % \
                    (self.current_downfact, self.obs.dt, \
                        self.current_downfact*self.obs.dt)

        # Calculate the appropriate initial dDM
        dDM = guess_DMstep(self.obs.dt*self.current_downfact, \
                            0.5*self.obs.BW, self.obs.fctr)
        if verbose:
            print "Best guess for optimal initial dDM is %.3f" % dDM
        while (self.get_next_dDM() < dDM):
            self.current_dDM = self.get_next_dDM()
        self.DDsteps.append(DDstep(self, self.current_downfact, \
                                    self.loDM, self.current_dDM, \
                                    numsub=self.numsub, \
                                    smearfact=SMEARFACT))
        
        # Calculate the next steps
        while self.DDsteps[-1].hiDM < self.hiDM:
            # Determine the new downsample factor
            self.current_downfact = self.get_next_downfact()
            eff_dt = self.obs.dt*self.current_downfact

            # Determine the new DM step
            while psr_utils.dm_smear(0.5*self.get_next_dDM(), self.obs.BW, \
                                        self.obs.fctr) < FF*eff_dt:
                self.current_dDM = self.get_next_dDM()

            # Get the next step
            self.DDsteps.append(DDstep(self, self.current_downfact, \
                                        self.DDsteps[-1].hiDM, \
                                        self.current_dDM, \
                                        numsub=self.numsub, \
                                        smearfact=SMEARFACT))

        # Calculate the predicted amount of time that will be spent in searching
        # this batch of DMs as a fraction of the total
        wfs = [step.numDMs/float(step.downsamp) for step in self.DDsteps]
        self.work_fracts = np.asarray(wfs)/np.sum(wfs)
                    

    def get_next_dDM(self):
        """Return the next avaialable DM step.
        """
        for dDM in ALLOW_DMSTEPS:
            if dDM > self.current_dDM:
                return dDM
        raise ValueError("No allowable DM steps left!")

    def get_next_downfact(self):
        """Return the next appropriate downsample factor.
        """
        index = self.obs.allow_factors.index(self.current_downfact)
        if (index+1) < len(self.obs.allow_factors):
            return self.obs.allow_factors[index+1]
        else:
            raise ValueError("No allowable downsample factors left!")

    def calc_min_smearing(self, verbose=False):
        """Calculate minimum (optimal) smearing.
            
            Inputs:
                verbose: Print information to screen (Default = False)
        """
        half_dDMmin = 0.5*ALLOW_DMSTEPS[0]
        self.min_chan_smear = psr_utils.dm_smear(self.loDM+half_dDMmin, \
                                            self.obs.chanwidth, self.obs.fctr)
        self.min_bw_smear = psr_utils.dm_smear(half_dDMmin, self.obs.BW, self.obs.fctr)
        self.min_total_smear = np.sqrt(2*self.obs.dt**2.0 + \
                                  self.min_chan_smear**2.0 + \
                                  self.min_bw_smear**2.0)
        self.best_resolution = max([self.req_resolution, self.min_chan_smear, \
                                    self.min_bw_smear, self.obs.dt])
        self.resolution = self.best_resolution
        
        if verbose:
            print
            print "Minimum total smearing     : %.3g s" % self.min_total_smear
            print "--------------------------------------------"
            print "Minimum channel smearing   : %.3g s" % self.min_chan_smear
            print "Minimum smearing across BW : %.3g s" % self.min_bw_smear
            print "Minimum sample time        : %.3g s" % self.obs.dt
            print
            print "Setting the new 'best' resolution to : %.3g s" % self.best_resolution

        # See if the data is too high time resolution for our needs
        if (FF*self.min_chan_smear > self.obs.dt) or \
                (self.resolution > self.obs.dt):
            if self.resolution > FF*self.min_chan_smear:
                if verbose:
                    print "   Note: resolution > dt (i.e. data is higher resolution than needed)"
                self.resolution = self.resolution
            else:
                if verbose:
                    print "   Note: min chan smearing > dt (i.e. data is higher resolution than needed)"
                self.resolution = FF*self.min_chan_smear

    def plot(self, fn=None):
        """Generate a plot for this dedispersion plan.

            Inputs:
                fn: Filename to save plot as. If None, show plot interactively.
                    (Default: Show plot interactively.)
        """
        fig = plt.figure(figsize=(11,8.5))
        ax = plt.axes()
        stepDMs = []
        # Plot each dedispersion step
        for ii, (step, wf) in enumerate(zip(self.DDsteps, self.work_fracts)):
            stepDMs.append(step.DMs)
            DMspan = np.ptp(step.DMs)
            loDM = step.DMs.min() + DMspan*0.02
            hiDM = step.DMs.max() - DMspan*0.02
            midDM = step.DMs.min() + DMspan*0.5
            # Sample time
            plt.plot(step.DMs, np.zeros(step.numDMs)+step.ddplan.obs.dt*step.downsamp, \
                        '#33CC33', label=((ii and "_nolegend_") or "Sample Time (ms)"))
            # DM stepsize smearing
            plt.plot(step.DMs, np.zeros(step.numDMs)+step.BW_smearing, 'r', \
                        label=((ii and "_nolegend_") or "DM Stepsize Smearing"))
            if self.numsub:
                plt.plot(step.DMs, np.zeros(step.numDMs)+step.sub_smearing, '#993399', \
                        label=((ii and "_nolegend_") or "Subband Stepsize Smearing (# passes)"))
            plt.plot(step.DMs, step.tot_smear, 'k', \
                        label=((ii and "_nolegend_") or "Total Smearing"))
            
            # plot text
            plt.text(midDM, 1.1*np.median(step.tot_smear), \
                        "%d (%.1f%%)" % (step.numDMs, 100.0*wf), \
                        rotation='vertical', size='small', \
                        ha='center', va='bottom')
            plt.text(loDM, 0.85*step.ddplan.obs.dt*step.downsamp, \
                        "%g" % (1000*step.ddplan.obs.dt*step.downsamp), \
                        size='small', color='#33CC33', ha='left')
            plt.text(hiDM, 0.85*step.BW_smearing, \
                        "%g" % step.dDM, size='small', color='r', ha='right')
            if self.numsub:
                plt.text(midDM, 0.85*step.sub_smearing, \
                            "%g (%d)" % (step.dsubDM, step.numprepsub), \
                            size='small', color='#993399', ha='center')
        allDMs = np.concatenate(stepDMs)
                                
        chan_smear = psr_utils.dm_smear(allDMs, self.obs.chanwidth, self.obs.fctr)
        bw_smear = psr_utils.dm_smear(ALLOW_DMSTEPS[0], self.obs.BW, self.obs.fctr)
        tot_smear = np.sqrt(2*self.obs.dt**2.0 + \
                                  chan_smear**2.0 + \
                                  bw_smear**2.0)
        plt.plot(allDMs, tot_smear, '#FF9933', label="Optimal Smearing")
        plt.plot(allDMs, chan_smear, 'b', label="Channel Smearing")
        # Add text above plot
        settings = r"$f_{ctr}$ = %g MHz" % self.obs.fctr
        if self.obs.dt < 1e-4:
            settings += r",  dt = %g $\mu$s" % (self.obs.dt*1e6)
        else:
            settings += r",  dt = %g ms" % (self.obs.dt*1000)
        settings += r",  BW = %g MHz" % self.obs.BW
        settings += r",  N$_{chan}$ = %d" % self.obs.numchan

        if self.numsub:
            settings += r",  N$_{sub}$ = %d" % self.numsub
        if self.obs.numsamp:
            settings += r",  N$_{samp}$ = %d" % self.obs.numsamp
        plt.figtext(0.05, 0.005, settings, \
                            ha='left', size='small')
    
        plt.yscale('log')
        plt.xlabel(r"Dispersion Measure (pc cm$^{-3}$)")
        plt.ylabel(r"Smearing (s)")
        plt.xlim(allDMs.min(), allDMs.max())
        plt.ylim(0.3*tot_smear.min(), 2.5*tot_smear.max())
        leg = plt.legend(loc='lower right')
        plt.setp(leg.texts, size='small')
        plt.setp(leg.legendHandles, linewidth=2)
        if fn is not None:
            # Save figure to file
            plt.savefig(fn, orientation='landscape', papertype='letter')
        else:
            # Show figure interactively
            def keypress(event):
                if event.key in ['q', 'Q']:
                    plt.close()
            fig.canvas.mpl_connect('key_press_event', keypress)
            plt.show()

    def __str__(self):
        lines = []
        if self.numsub:
            lines.append("\n  Low DM    High DM     dDM  DownSamp  dsubDM   #DMs  DMs/call  calls  WorkFract")
        else:
            lines.append("\n  Low DM    High DM     dDM  DownSamp   #DMs  WorkFract")
        for (ddstep, wf) in zip(self.DDsteps, self.work_fracts):
            lines.append("%s   %.4g" % (ddstep, wf))
        lines.append("\n")
        return "\n".join(lines)

                
def guess_DMstep(dt, BW, fctr):
    """Choose a reasonable DMstep by setting the maximum smearing across the
        'BW' to equal the sampling time 'dt'.
        
        Inputs:
            dt: sampling time (in seconds)
            BW: bandwidth (in MHz)
            fctr: centre frequency (in MHz)
    """
    return dt*0.0001205*fctr**3.0/(BW)
                                
        
def main():
    obs = Observation(options.dt, options.fctr, options.bandwidth, \
                        options.numchan, options.numsamp)
    ddplan = obs.gen_ddplan(options.loDM, options.hiDM, \
                            options.numsub, options.res, verbose=True)
    print ddplan
    # Plot ddplan
    ddplan.plot(fn=options.outfile)

if __name__ == '__main__':
    parser = optparse.OptionParser(prog="DDplan2.py", \
                version="v2.0beta Patrick Lazarus "
                        "(Sept. 23, 2010 - based on Scott Ransom's DDplan.py)", \
                description="The program generates a good plan for " \
                            "de-dispersing raw data. It trades a small " \
                            "amount of sensitivity in order to " \
                            "save computation costs.")
    parser.add_option('-o', '--outfile', dest='outfile', default=None, \
                        help="Output plot file (default is Xwin).")
    parser.add_option('-l', '--loDM', dest='loDM', type='float', default=0.0, \
                        help="Low DM to search (default = 0 pc cm-3).")
    parser.add_option('-d', '--hiDM', dest='hiDM', type='float', default=1000.0, \
                        help="High DM to search (defaut = 1000 pc cm-3).")
    parser.add_option('-f', '--fctr', dest='fctr', type='float', default=1400.0, \
                        help="Centre frequency (default = 1400 MHz).")
    parser.add_option('-b', '--bw', dest='bandwidth', type='float', default=300.0, \
                        help="Observing bandwidth (default = 300 MHz).")
    parser.add_option('-n', '--numchan', dest='numchan', type='int', default=1024, \
                        help="Number of frequency channels (default = 1024).")
    parser.add_option('-t', '--dt', dest='dt', type='float', default=64e-6, \
                        help="Sample time (default = 64e-6 s).")
    parser.add_option('-s', '--subbands', dest='numsub', type='int', default=0, \
                        help="Number of subbands (default = numchan).")
    parser.add_option('-p', '--numsamp', dest='numsamp', type='int', default=0, \
                        help="Number of samples (or samps per row for PSRFITS data).")
    parser.add_option('-r', '--res', dest='res', type='float', default=None, \
                        help="Acceptable time resultion (in ms, required).")
    (options, sys.argv) = parser.parse_args()

    if options.res is None:
        sys.stderr.write("An acceptable time resolution _must_ be provided.\n")
        sys.stderr.write("Please use -r/--res option on commandline.\n")
        sys.exit(1)

    main()
