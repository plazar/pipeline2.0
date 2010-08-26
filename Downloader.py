import os.path
from ftplib import FTP
from time import sleep
import sys
import os
from suds.client import Client
from threading import Thread
import shutil

from config import downloader_temp, rawdata_directory

#TODO configuration reading from pipiline config
# 1 - Space asvailable

class DownloadModule:

    def __init__(self,rpc_url):
        self.WebService =  Client(rpc_url).service
        self.password = 'myTestPassword'
        self.downloaders = []
        self.restores = []
        print downloader_temp

    def run(self):
        while True:
            if self.have_space():
                response = self.restore()
                print "Requested Restore"
                if response != "Error":
                    #self.downloaders.append(downloader())
                    print "Restore Response: "+ response
                    self.restores.append(response)
                for restore in self.restores:
                    self.getLocation(restore)
                for download in self.downloaders:
                    if download.status == 'New':
                        download.start()
            for download in self.downloaders[:]:
                print download.status
                if download.status.split(':')[0] == "Downloaded" or \
                download.status.split(':')[0] == 'Failed':
                    self.downloaders.remove(download)
            sleep(3)

    def getLocation(self,restore):
        response = self.WebService.LocationTest(pw=self.password, guid=restore)
        if response != "incomplete" and response  != "Error":
#           if not self.downloading(response):
#               self.downloaders.append(downloader(response))
#               self.restores.remove(restore)

          if not self.downloading('10m'):
            self.downloaders.append(downloader('10m'))
            self.restores.remove(restore)
          else:
            self.restores.remove(restore)
    
    def downloading(self,file_name):
        for download in self.downloaders:
            if download.file_name == file_name:
                return True
        return False

    def restore(self):
        return self.WebService.RestoreTest(pw=self.password,number=30)


    def have_space(self):
        return True

class downloader(Thread):

    def __init__(self,file_path):
        Thread.__init__(self)
        self.ftp = FTP('ftp.bic.mni.mcgill.ca')
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.file = None
        if not os.path.exists(os.path.join(rawdata_directory,self.file_name)):
            self.ftp.login()
            self.download = True
            self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
            self.ftp.cwd(os.path.dirname(self.file_path))
            self.status = 'New'
        else:
            self.download = False
            self.finished("Failed: File '"+self.file_name+"' already exists")

        
        self.total_size_got = 0
        self.file_size = 0
        
        
    def run(self):
        if self.download:
            self.get_file_size()
            print self.file_size
            try:
                self.ftp.retrbinary("RETR "+self.file_path, self.write)
                shutil.move(os.path.join(downloader_temp,self.file_name),rawdata_directory)
                self.finished('Downloaded: '+str(self.total_size_got)+" -- "+ str((float(self.total_size_got) / float(self.file_size)) * 100 ) +"%")
            except Exception as e:
                self.finished('Failed: '+str(e))
        



    def get_file_size(self):
        self.file_size = self.ftp.size(self.file_path)

    def write(self, block):
#        print len(block)
        sleep(0.01)
        self.total_size_got += len(block)
        self.status = 'Downloading: '+str(self.total_size_got)+" -- "+ str(int(float(self.total_size_got) / float(self.file_size) * 100 )) +"%"
        self.file.write(block)

    def finished(self,message):
        #print "Closing File: "+self.file_name
        self.status = message
        if self.file:
            self.file.close()
        self.ftp.close()

    def status(self):
        print "File: "+self.file_name
        print "Downloaded: "+ str(self.total_size_got)
        print ""

controller = DownloadModule('http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL')
controller.run()