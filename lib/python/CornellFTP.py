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
        return self.nlst(ftp_path)

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
        response = self.sendcmd("MDTM %s")
        respcode = response.split()[0]
        if respcode=='213':
            # All's good
            # Parse first 14 characters of date string
            # (Characters 15+ are fractions of a second)
            modstr = response.split()[1][:14]
            modtime = datetime.strptime(modstr, "%Y%m%d%H%M%S")
        else:
            raise get_ftp_exception(response)
        return modtime

    def download(self, ftp_path, local_path=config.download.datadir):

        localfn = os.path.join(local_path, os.path.basename(ftp_path))


        if config.download.use_lftp:

            username = config.download.ftp_username
            password = config.download.ftp_password

            lftp_cmd = '"get %s -o %s"' % (ftp_path, localfn)
            cmd = "lftp -c 'set xfer:clobber 1; open -e %s -u %s,%s " %\
                     (lftp_cmd, username, password)\
                     + "-p 31001 arecibo.tc.cornell.edu'"

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


def get_ftp_exception(msg):
    """Return a CornellFTPError or a CornellFTPTimeout depending
        on the string msg.

        Input:
            msg: The exception message to be used. 

        Output:
            exc: The exception instance to be raised.
    """
    if "[Errno 110] Connection timed out" in msg or "[Errno 113] No route to host" in msg:
        exc = CornellFTPTimeout(msg)
    else:
        exc = CornellFTPError(msg)
    return exc


class CornellFTPError(pipeline_utils.PipelineError):
    pass


class CornellFTPTimeout(pipeline_utils.PipelineError):
    pass
