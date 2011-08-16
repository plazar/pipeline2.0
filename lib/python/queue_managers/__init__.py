import generic_interface
import pipeline_utils

class QueueManagerFatalError(pipeline_utils.PipelineError):
    """This error should be used when the queue manager has
        a fatal error. The queue manager will be stopped.
        The job/action currently being processed will be left
        in whatever state it is in at the time the error occurred.
    """
    pass


class QueueManagerJobFatalError(pipeline_utils.PipelineError):
    """This error should be used when the queue manager has an
        error that should cause the job being processed to be
        marked as 'failed', but the queue manager can continue
        running.
    """
    pass


class QueueManagerNonFatalError(pipeline_utils.PipelineError):
    """This error should be used when the queue manager demonstrates
        some behaviour that is considered non-fatal to both the
        queue manager and the job/action that was being processed.
    """
    pass
    

