#!/usr/bin/env python
import sys
import warnings
import prettytable

import pyodbc

import config.commondb

# Connecting from Linux
DATABASES = {
    'common': {
        'DATABASE': 'palfa-common',
        'UID':  config.commondb.username,
        'PWD':  config.commondb.password,
        'HOST': config.commondb.host,
        'DSN':  'FreeTDSDSN'
        },
    'common-copy': {
        'DATABASE': 'palfa-common-copy',
        'UID':  config.commondb.username,
        'PWD':  config.commondb.password,
        'HOST': config.commondb.host,
        'DSN':  'FreeTDSDSN'
        },
}


# Set defaults
DEFAULTDB = 'common-copy'
DATABASES['default'] = DATABASES[DEFAULTDB]


class Database:
    """Database object for connecting to databases using pyodbc.
    """
    def __init__(self, db="default", autocommit=True):
        """Constructor for Database object.
            
            Input:
                'db': database to connect to. (Default: 'default')
                        (gets passed to 'self.connect')
                'autocommit': boolean, determines if autocommit should
                                be turned on or off.
        """
        self.db = db
        self.connect(db, autocommit=autocommit)
    
    def connect(self, db="default", autocommit=True):
        """Establish a database connection. Set self.conn and self.cursor.
            
            Input:
                'db': databse to connect to. Must be a key in module's
                        DATABASES dict. (Default: 'default')
                'autocommit': boolean, determines if autocommit should
                                be turned on or off.
            Output:
                None
        """
        if db not in DATABASES:
            warnings.warn("Database (%s) not recognized. Using default (%s)." \
                            % (db, DEFAULTDB))
            db = 'default'
        try:
            self.conn = pyodbc.connect(autocommit=autocommit, **DATABASES[db])   
            self.cursor = self.conn.cursor()
        except:
            msg  = "Could not establish database connection.\n"
            msg += "\tCheck your connection options:\n"
            for key, val in DATABASES[db].iteritems():
                msg += "\t\t%s: %s\n" % (key, val)

            raise DatabaseConnectionError(msg)

    def execute(self, *args, **kwargs):
        self.cursor.execute(*args, **kwargs)

    def commit(self):
        self.conn.commit()
  
    def rollback(self):
        self.conn.rollback()
  
    def close(self):
        """Close database connection.
        """
        try:
            self.conn.close()
        except ProgrammingError:
            # database connection is already closed
            pass

    def showall(self):
        desc = self.cursor.description
        if desc is not None:
            fields = [d[0] for d in desc] 
            table = prettytable.PrettyTable(fields)
            for row in self.cursor:
                table.add_row(row)
        table.printt()

    def insert(self, query):
        self.cursor.execute(query)
        self.commit()
    
    def findFirst(self, query, dict_result = True):
        self.cursor.execute(query)    
        row = self.cursor.fetchone()
        if dict_result:
            names = [desc[0] for desc in self.cursor.description] 
            dict_rows = dict()
            if row:
                i = 0        
                for name in names:  
                    dict_rows[name] = row[i]
                    i = i + 1
        else:
           dict_rows = row 
            
        return dict_rows

    def findAll(self, query, dict_result = True):
        self.cursor.execute(query)        
        rows = self.cursor.fetchall()
        if dict_result:
            names = [desc[0] for desc in self.cursor.description] # cursor.description contains other info (datatype, etc.)
            dict_rows = [dict(zip(names, vals)) for vals in rows]    
        else:
            dict_rows = rows
            
        return dict_rows
        

    def findBlob(self):
        candidate = self.findFirst("select filename, filedata  from PDM_Candidate_plots where pdm_plot_type_id = 2;")
        #c2 = self.findFirst("select datalength(filedata) as b  from PDM_Candidate_plots where pdm_plot_type_id = 2;")            
        file = open(candidate["filename"], 'wb')        
        file.write(candidate["filedata"])
        file.close()
        
        return candidate["filename"]

    
    def findBlobLimit(self, id):
        results = self.findAll("select top 15 pdm_cand_id from PDM_Candidate_plots where pdm_plot_type_id = 2 and pdm_cand_id > " + str(id) + ";")
        print results
        for result in results:
            candidate = self.findFirst("select filename, filedata  from PDM_Candidate_plots where pdm_plot_type_id = 2 and pdm_cand_id = " + str(result["pdm_cand_id"]) + ";")
            print candidate["filename"]
            file = open(candidate["filename"], 'wb')        
            file.write(candidate["filedata"])
            file.close()
        return candidate["filename"]    


class DatabaseConnectionError(Exception):
    pass


if __name__=='__main__':
    if len(sys.argv) > 1:
        print "Connecting to", sys.argv[1]
        db = Database(sys.argv[1])
    else:
        print "Connecting to", DEFAULTDB
        db = Database()
    print "Connected!"
    print "conn:", db.conn
    print "cursor:", db.cursor
    db.close()
