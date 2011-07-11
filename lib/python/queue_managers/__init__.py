import generic_interface
import pipeline_utils

class QueueManagerFatalError(pipeline_utils.PipelineError):
    pass


class QueueManagerNonFatalError(pipeline_utils.PipelineError):
    pass
    

