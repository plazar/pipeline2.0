import subprocess
import time
merge_pipe = subprocess.Popen('ping -c 1 google.ca', \
shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)

response = merge_pipe.communicate()

print "Response list length: "+str(len(response))
print "STOUT length: "+ str(len(response[0]))
print "STOUT: "+ str(response[0])
print "STERR: "+ str(response[1])

mylist = ['sasha','sveta','natasha','misha','grisha']

for name in mylist[:]:
    print str(len(mylist))
    print name
    mylist.remove(name)
    print str(len(mylist))