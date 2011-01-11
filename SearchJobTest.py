import job

datafile = ['/data/alfa/test_pipeline/p2030_54553_43341_0091_G50.62-00.03.S_2.w4bit.fits']

print "Creating the job..."
sj = job.PulsarSearchJob(datafile)
print "Submitting the job to QSUB..."
sj.submit()
print "Qsub jobid: %s" % sj.jobid
print "Getting QSUB status of the job..."
sj.get_qsub_status()
print "Status: %s " % str(sj.status)
print "Removing the job from QSUB..."
print "Removed?: %s" % sj.delete()
print "Errors while running the job?: %s" % sj.queue_error()