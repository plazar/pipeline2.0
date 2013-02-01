import os
import os.path

import M2Crypto

import pipeline_utils
import mailer
import OutStream
import config.basic
import config.background
import config.download
import subprocess
import datetime

cout = OutStream.OutStream("CornellFTP Module", \
                os.path.join(config.basic.log_dir, "downloader.log"), \
                config.background.screen_output)

class CornellFTP(M2Crypto.ftpslib.FTP_TLS):
    def __init__(self, host=config.download.ftp_host, \
                        port=config.download.ftp_port, \
                        username=config.download.ftp_username, \
                        password=config.download.ftp_password, \
                        *args, **kwargs):

        M2Crypto.ftpslib.FTP_TLS.__init__(self, *args, **kwargs)
        try:
            self.connect(host, port)
            self.auth_tls()
            self.set_pasv(1)
            self.login(username, password)
        except Exception, e:
            raise get_ftp_exception(str(e))
        else:
            cout.outs("CornellFTP - Connected and logged in")

    def __del__(self):
        if self.sock is not None:
            self.quit()

    def list_files(self, ftp_path):
        try:
            flist = self.nlst(ftp_path)
        except Exception, e:
            cout.outs("CornellFTP - FTP list_files failed: %s" % \
                        (str(e)))
            raise get_ftp_exception("Could not list files in (%s) " 
                                    "on FTP server: %s" % (ftp_path, str(e)))
        return flist

    def get_files(self, ftp_path):
        files = self.list_files(ftp_path)
        sizes = [self.size(os.path.join(ftp_path, fn)) for fn in files]
        return zip(files, sizes)

    def get_modtime(self, ftp_path):
        """Given a file's path on the FTP server return
            a datetime.datetime object that represents the file's
            time of last modification. This is gotten by using 
            the MDTM command. Fractions of a second are ignored.

            Modification time is returned in GMT.

            Input:
                ftp_path: path of file on FTP server.

            Output:
                modtime: datetime.datetime object that encodes the file's
                    last time of modification.
        """
        response = self.sendcmd("MDTM %s" % ftp_path)
        respcode = response.split()[0]
        if respcode=='213':
            # All's good
            # Parse first 14 characters of date string
            # (Characters 15+ are fractions of a second)
            modstr = response.split()[1][:14]
            modtime = datetime.datetime.strptime(modstr, "%Y%m%d%H%M%S")
        else:
            raise get_ftp_exception(response)
        return modtime

    def dir_exists(self,ftp_dir):
        """Check to see if directory exists on FTP server.
            Note: Checks for existence of all dirs in a path.

            Input: 
                ftp_dir: path of directory on FTP server.

            Output:
                exists: boolean of whether exists or not.
        """
        dir_list = ftp_dir.split("/")
        test_dir = ""
        for i in range(len(dir_list) - 1):
          test_dir += dir_list[i] + "/"
          filelist = self.list_files(test_dir) 
          exists = dir_list[i+1] in filelist
          if not exists:
              return False
        return True

    def get_size(self,ftp_fn):
        """Get size of file on FTP server.

            Input: 
                ftp_fn: filename of file on FTP server.

            Output:
                size of file.
        """
        try:
            return self.size(ftp_fn)
        except Exception, e:
            raise get_ftp_exception("Could not get size of %s " 
                                    "on FTP server: %s" % (ftp_fn, str(e)))
            

    def download(self, ftp_path, local_path=config.download.datadir,\
                 preserve_modtime=True):

        localfn = os.path.join(local_path, os.path.basename(ftp_path))


        if config.download.use_lftp:

            username = config.download.ftp_username
            password = config.download.ftp_password

            lftp_cmd = '"get %s -o %s"' % (ftp_path, localfn)
            
            if preserve_modtime:
                cmd = "lftp -c 'set xfer:clobber 1;"
            else:
                cmd = "lftp -c 'set xfer:clobber 1; set ftp:use-mdtm 0;"

            cmd += " open -e %s -u %s,%s " % (lftp_cmd, username, password)\
                   + "-p 31001 arecibo.tc.cornell.edu' > /dev/null"

            cout.outs("CornellFTP - Starting Download of: %s" % \
                        os.path.split(ftp_path)[-1])

            subprocess.call(cmd, shell=True)

        else:
            f = open(localfn, 'wb')
        
            # Define a function to write blocks to the file
            def write(block):
                f.write(block)
                #f.flush()
                #os.fsync(f)
        
            self.sendcmd("TYPE I")
            cout.outs("CornellFTP - Starting Download of: %s" % \
                        os.path.split(ftp_path)[-1])
            self.retrbinary("RETR "+ftp_path, write)
            f.close()

        cout.outs("CornellFTP - Finished download of: %s" % \
                        os.path.split(ftp_path)[-1])
        return localfn 

    def upload(self, local_path, ftp_path):
        f = open(local_path, 'r')
        
        self.sendcmd("TYPE I")
        cout.outs("CornellFTP - Starting upload of: %s" % \
                    os.path.split(local_path)[-1])
        try:
            self.storbinary("STOR "+ftp_path, f)
        except Exception, e:
            cout.outs("CornellFTP - Upload of %s failed: %s" % \
                        (os.path.split(local_path)[-1], str(e)))
            raise get_ftp_exception("Could not store binary file (%s) " 
                                    "on FTP server: %s" % (ftp_path, str(e)))
        else:
            cout.outs("CornellFTP - Finished upload of: %s" % \
                        os.path.split(local_path)[-1])
        finally:
            f.close()
        
        # Check the size of the uploaded file
        ftp_size = self.size(ftp_path)
        local_size = os.path.getsize(local_path)
        if ftp_size == local_size:
            cout.outs("CornellFTP - Upload of %s successful." % \
                    os.path.split(local_path)[-1])
        else:
            cout.outs("CornellFTP - Upload of %s failed! " \
                        "File sizes of local file and " \
                        "uploaded file on FTP server " \
                        "don't match (%d != %d)." % \
                        (os.path.split(local_path)[-1], local_size, ftp_size))
            raise get_ftp_exception("File sizes of local file and " \
                                    "uploaded file on FTP server " \
                                    "don't match (%d != %d)." % \
                                    (local_size, ftp_size))

def mirror(source_dir,dest_dir,reverse=False,parallel=10):
    """Use the lftp mirror command to mirror a local/remote directory to a 
        remote/local directory. To download (remote to local) use reverse=True,
        otherwise set as False (the default).
    """
    username=config.download.ftp_username
    password=config.download.ftp_password

    # make sure ends with '/' so that directory is copied, and not individual files
    if not dest_dir.endswith('/'):
        dest_dir += '/'

    reverse_flag = '-R' if reverse else ''

    cout.outs("CornellFTP - Starting lftp mirror of: %s" % \
                os.path.split(source_dir)[-1])

    lftp_cmd = '"mirror %s --parallel=%d %s %s"' % \
                (reverse_flag,parallel,source_dir,dest_dir)
    cmd = "lftp -c 'open -e %s -u %s,%s " %\
             (lftp_cmd, username, password)\
             + "-p 31001 arecibo.tc.cornell.edu'"
    subprocess.call(cmd, shell=True)

    cout.outs("CornellFTP - Finished lftp mirror of: %s" % \
                os.path.split(source_dir)[-1])

def pget(ftp_fn, local_path, parallel=10):
    """Use the lftp pget command to download a from from the FTP server to a 
        local directory. 

        Input: 
            ftp_fn: filename of file on FTP server.
            local_path: local path where file will be to downloaded.
    """
    username=config.download.ftp_username
    password=config.download.ftp_password

    cout.outs("CornellFTP - Starting lftp pget of: %s" % \
                os.path.split(ftp_fn)[-1])

    lftp_cmd = '"pget -n %d %s -o %s"' % \
                (parallel,ftp_fn,local_path)
    cmd = "lftp -c 'set xfer:clobber 1; open -e %s -u %s,%s " %\
             (lftp_cmd, username, password)\
             + "-p 31001 arecibo.tc.cornell.edu'"
    subprocess.call(cmd, shell=True)

    cout.outs("CornellFTP - Finished lftp pget of: %s" % \
                os.path.split(ftp_fn)[-1])

def get_ftp_exception(msg):
    """Return a CornellFTPError or a CornellFTPTimeout depending
        on the string msg.

        Input:
            msg: The exception message to be used. 

        Output:
            exc: The exception instance to be raised.
    """
    if "[Errno 110] Connection timed out" in msg\
       or "[Errno 113] No route to host" in msg\
       or "104, 'Connection reset by peer'" in msg:
        exc = CornellFTPTimeout(msg)
    else:
        exc = CornellFTPError(msg)
    return exc


class CornellFTPError(pipeline_utils.PipelineError):
    pass


class CornellFTPTimeout(pipeline_utils.PipelineError):
    pass
