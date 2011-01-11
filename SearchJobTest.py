import job

datafile = ['/data/alfa/test_pipeline/p2030_54553_43341_0091_G50.62-00.03.S_2.w4bit.fits']
sj = job.PulsarSearchJob(datafile)
sj.submit()
sj.get_qsub_status()
print "status: %s " % str(sj.status)

