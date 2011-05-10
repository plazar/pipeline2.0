import jobtracker
"""
This script displays files that are currently being downloader by
Downloader module with a total number of active downloads.
"""

def main():
    downloading = jobtracker.query("SELECT * FROM files, download_attempts WHERE download_attempts.status='downloading' AND files.id=download_attempts.file_id")
    for download in downloading:
        print "%s\t\t%s" % (download['remote_filename'],download['details'])

    print "\nTotal: %u" % len(downloading)


if __name__ == "__main__":
    main()
