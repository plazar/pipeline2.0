#!/usr/bin/python

import sqlite3
import getopt, sys
import traceback


opts, extraparams = getopt.getopt(sys.argv[1:],'d')
try:
    conn = sqlite3.connect('sqlite3db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
except Exception, e:
    print "Error occured while querying the database: %s ." % str(e)
    traceback.print_exc()


all_rows = []

def downloading():
    try:
        query = "SELECT * FROM restore_downloads WHERE status LIKE 'Downloading%'"
        cur.execute(query)
        return cur.fetchall()
    except Exception, e:
        print "Error occured while querying the database: %s ." % str(e)
        traceback.print_exc()
        
for o,a in opts:
    if o == "-d":
        all_rows.append(downloading())

for row in all_rows:
    print "Restore ID: %s" % (row[0]['guid'])
    print "Filename: %s" % (row[0]['filename'])
    print "Nm. of download tries: %s" % (row[0]['num_tries'])
    print "Status: %s" % (row[0]['status'])    


    
    
