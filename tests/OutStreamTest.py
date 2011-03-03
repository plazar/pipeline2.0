
from OutStream import OutStream as OutStream

l = OutStream("Tester","test.log",True)
l1 = OutStream("JobTestlogger","test.log",True)
l.outs("tesing testing 1...2...3..",OutStream.WARNING)

datafile = 'fgqerwgerwgwre'
str = "DEBUG: [datafile] when creating PulsarSearchJob", [datafile]
l1.outs(str,OutStream.INFO)
