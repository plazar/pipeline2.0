import M2Crypto
import config.download
import os

class CornellFTP():

    def __init__(self):
        self.username = config.download.ftp_username
        self.password = config.download.ftp_password
        self.host =config.download.ftp_host
        self.port =config.download.ftp_port

    def get_files(self,ftp_path):
        myFtp = self.login(self.connect())
        files_list = self.list_files(myFtp,ftp_path)
        files_and_size = list()
        for file in files_list:
            files_and_size.append( (file, myFtp.size(os.path.join(ftp_path,file)) ) )
        myFtp.close()
        return files_and_size

    def download(self,ftp_file_path):
        myFtp = self.login(self.connect())
        self.downloading_file = open(os.path.join(os.path.basename(ftp_file_path)),'wb')
        myFtp.sendcmd("TYPE I")
        myFtp.retrbinary("RETR "+ftp_file_path, self.write)
        self.downloading_file.close()
        myFtp.close()
        return True

    def connect(self):
        myFtp = M2Crypto.ftpslib.FTP_TLS()
        myFtp.connect(self.host, self.port)
        myFtp.auth_tls()
        myFtp.set_pasv(1)
        print "Connected."
        return myFtp

    def login(self,connection):
        print self.username,self.password
        connection.login('palfadata','NAIC305m')
        #(self.username, self.password)
        print "Logged in."
        return connection

    def list_files(self,login_connection,ftp_path):
        if not login_connection:
            raise Exception('Not Connected')
        return login_connection.nlst(ftp_path)

    def write(self,block):
        self.downloading_file.write(block)