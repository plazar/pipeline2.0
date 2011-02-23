import M2Crypto
import config.download
import os
import mailer
import OutStream
cout = OutStream.OutStream("CornellFTP Module",config.download.log_file_path, config.background.screen_output).outs

class CornellFTP():

    def __init__(self):
        self.username = config.download.ftp_username
        self.password = config.download.ftp_password
        self.host =config.download.ftp_host
        self.port =config.download.ftp_port

    def get_files(self,ftp_path):
        try_counter = 0
        login=False
        while not login:
            try_counter+=1
            try:
                myFtp = self.login(self.connect())
                files_list = self.list_files(myFtp,ftp_path)
                files_and_size = list()
                login = True
            except (CornellFTPConnectionError,CornellFTPLoginError),e:
                print str(e)
                if try_counter > 7:
                    try:
                        notification = mailer.ErrorMailer('CornellFTP login failure, retried %u times: %s' % (str(e),try_counter) )
                        notification.send()
                    except Exception,e:
                        pass

        for file in files_list:
            files_and_size.append( (file, myFtp.size(os.path.join(ftp_path,file)) ) )
        myFtp.close()
        return files_and_size

    def download(self,ftp_file_path):
        try_counter = 0
        login=False
        while not login:
            try_counter+=1
            try:
                myFtp = self.login(self.connect())
                login = True
            except (CornellFTPConnectionError,CornellFTPLoginError),e:
                print str(e)
                if try_counter > 7:
                    try:
                        notification = mailer.ErrorMailer('CornellFTP login failure, retried %u times: %s' % (str(e),try_counter) )
                        notification.send()
                    except Exception,e:
                        pass
        self.downloading_file = open(os.path.join(config.download.temp,os.path.basename(ftp_file_path)),'wb')
        myFtp.sendcmd("TYPE I")
        cout("CornellFTP - Starting Download of: %s" % ftp_file_path)
        myFtp.retrbinary("RETR "+ftp_file_path, self.write)
        self.downloading_file.close()
        myFtp.close()
        return True

    def connect(self):
        try:
            myFtp = M2Crypto.ftpslib.FTP_TLS()
            myFtp.connect(self.host, self.port)
            myFtp.auth_tls()
            myFtp.set_pasv(1)
            cout("CornellFTP - Connected.")
        except Exception,e:
            raise CornellFTPConnectionError( "%s" % str(e) )
        return myFtp

    def login(self,connection):
        try:
            connection.login(self.username, self.password)
            cout("CornellFTP - Logged in.")
            return connection
        except Exception, e:
            raise CornellFTPLoginError( "%s" % str(e) )

    def list_files(self,login_connection,ftp_path):
        if not login_connection:
            raise Exception('CornellFTP - Not Connected')
        return login_connection.nlst(ftp_path)

    def write(self,block):
        self.downloading_file.write(block)


class CornellFTPConnectionError(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class CornellFTPLoginError(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)