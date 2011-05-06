#!/usr/bin/env python

import random
import database

print "Connecting to common DB"
db = database.Database('common')
print "Connection established"
print "conn:", db.conn
print "cursor:", db.cursor

query = "SELECT COUNT(*) " \
        "FROM headers " \
        "WHERE source_name LIKE 'G%'"
print "\nPerforming SELECT query:", query
db.execute(query)
print db.cursor.fetchone()[0]

randint = random.randint(0,1000)
randfloat = randint/1000.0
teststring = "This is a test"
query = "INSERT INTO Test_table (" \
             "test_int, " \
             "test_float, " \
             "test_string) " \
        "VALUES (%d, %f, '%s') " % \
            (randint, randfloat, teststring)
print "\nInserting to test table:", query
db.execute(query)

query = "SELECT test_id, test_int, test_float, test_string " \
        "FROM Test_table " \
        "WHERE test_id=@@IDENTITY"
print "\nRetrieving the insert:", query
db.execute(query)
id, i, f, s = db.cursor.fetchone()
print "ID assigned: %d" % id
if (i != randint) or (f != randfloat) or (s != teststring):
    raise ValueError("Inserted and retrieved values don't match!\n" \
                     "(insert -- retrieved: %d -- %d; %f -- %f; %s -- %s)" % \
                     (randint, i, randfloat, f, teststring, s))
else:
    print "\nData retrieved successfully!"

db.close()
