import config.jobpool
import job

pool = job.JobPool()

pool.recover_from_qsub(testing=True)
print "Jobs in JobPool: %s" % str(len(pool.jobs))
print "Jobs running: %s  Jobs queued: %s" % config.jobpool.queue_manager.status()
print "Datafiles in JobPool: %s" % str(len(pool.datafiles))
for filename in pool.datafiles:
    print filename
