#import CornellFTP
#
#C_FTP = CornellFTP.CornellFTP()
#
#print C_FTP.get_files('e96ea139361740eca91f0f82ed4d889f')
#del C_FTP
#
#
import DownloaderRF
#res = DownloaderRF.restore(1,guid='e96ea139361740eca91f0f82ed4d889f')
#
#print res.create_downloads()

dl = DownloaderRF.DownloadModule()
dl.run()