from QsubManager import Qsub

jobid = Qsub.submit(['/path/to/the/data/file'],'/output/directory')
print jobid
print Qsub.is_running(jobid)
print Qsub.status()
print Qsub.delete(jobid)

