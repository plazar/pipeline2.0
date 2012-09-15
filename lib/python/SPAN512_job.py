import datafile
import os
import pipeline_utils
import config.processing

def presubmission_check(fns):
    """Check to see if datafiles meet the critera for submission.
    """
    # Check that files exist
    missingfiles = [fn for fn in fns if pipeline_utils.get_file_size(fn) <=0 ]
    if missingfiles:
        errormsg = "The following files cannot be found:\n"
        for missing in missingfiles:
            errormsg += "\t%s\n" % missing
        raise pipeline_utils.PipelineError(errormsg) # if files missing want to crash

    #check if observation is too short
    #limit = float(config.jobpooler.obstime_limit)
    #obs_time = data.observation_time
    #if obs_time < limit:
    #    errormsg = 'Observation is too short (%.2f s < %.2f s)' % (obs_time, limit) 
    #    raise FailedPreCheckError(errormsg)
    #check if datafile has been successfully processed in the past



def get_output_dir(fns):
    """Given a list of data files, 'fns', generate path to output results.

        path is:
            {base_results_directory}/{mjd}/{obs_name}/{beam_num}/{proc_date}/
        Note: 'base_results_directory' is defined in the processing config file.
                'mjd', 'obs_name', and 'beam_num' are from parsing
                the job's datafiles. 'proc_date' is the current date
                in yymmddThhmmss format.
    """

    # Generate output directory
    path, filename = os.path.split(fns[0])
    filename, suffix = filename.split('.')
    
    baseoutdir = os.path.join(config.processing.base_results_directory, \
                                    filename)
    outdir = baseoutdir
    
    # Make sure our output directory doesn't already exist
    #counter = 0
    #while os.path.exists(outdir):
    #    counter += 1
    #    outdir = "%s_%d" % (baseoutdir, counter)
    
    # Make the directory immediately so the pipeline knows it's taken
    os.makedirs(outdir)

    # Send an email if our first choice for outdir wasn't available
    #if counter:
    #    errormsg = "The first-choice output directory '%s' " \
    #                "already existed. Had to settle for '%s' " \
    #                "after %d tries. \n\n " \
    #                "Data files:\n " \
    #                "\t%s" % (baseoutdir, outdir, counter, "\n\t".join(fns))
    #    notification = mailer.ErrorMailer(errormsg, \
    #                    subject="Job outdir existance warning")
    #    notification.send()
    return outdir




def is_complete(fns):
    """Return True if the list of file names, 'fns' is complete.
        
        Inputs:
            fns: A list of file names.

        Output:
            complete: Boolean value. True if list of file names
                is a group that is complete.
    """
    if len(fns)==1:
        return True
    else:
        return False


