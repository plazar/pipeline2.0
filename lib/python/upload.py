"""
Module to be used to upload to the PALFA common database.

Patrick Lazarus, Jan. 12, 2011
"""
import sys
import atexit
import warnings

import database


# A global dictionary to keep track of database connections
db_connections = {}
            

@atexit.register # register this function to be executed at exit time
def close_db_connections():
    """A function to close database connections at exit time.
    """
    for db in db_connections.values():
        db.close()


class Uploadable(object):
    """An object to support basic operations for uploading
        to the PALFA commonDB using SPROCs.
    """
    def get_upload_sproc_call(self):
        raise NotImplementedError("get_upload_sproc_call() should be defined by a " \
                                  "subclass of Uploadable.")
    
    def upload(self, dbname='common-copy'):
        """Upload an Uploadable to the desired database.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'common-copy').
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            warnings.warn("Default is to connect to common-copy DB "
                            "at Cornell for testing...")
            if dbname not in db_connections:
                db_connections[dbname] = database.Database(dbname)
            db = db_connections[dbname]
        query = str(self.get_upload_sproc_call())
        try:
            db.cursor.execute(query)
        except:
            raise UploadError("There was an error executing the following " \
                                "query: %s" % query[:256])
        try:
            result = db.cursor.fetchone()[0]
        except:
            raise UploadError("There was an error fetching the result of " \
                                "the following query: %s" % query[:256])
        return result

    def __str__(self):
        s = self.get_upload_sproc_call()
        return s.replace('@', '\n    @')
 

class UploadError(Exception):
    """An error to do with uploading to the PALFA common DB.
        In most instances, this error will wrap an error thrown
        by pyodbc.
    """
    def __init__(self, *args):
        super(UploadError, self).__init__(self, *args)
        self.orig_exc = sys.exc_info() # The exception being wrapped, if any
