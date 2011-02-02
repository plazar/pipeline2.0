import DownloaderRF
import time
"""res = DownloaderRF.restore(db_name='storage_db',num_beams=1)

cur = res.request()

res.update_from_db()
print res.values

while not res.is_finished():
    res.run()
    res.status()
    time.sleep(3)
"""


dlm = DownloaderRF.DownloadModule()
dlm.run()

