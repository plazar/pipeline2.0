import sys
import sqlite3
import time
import datetime
import types

import prettytable

import debug 
import config.background


class JobtrackerDatabase(object):
    """An object to interface with the jobtracker database.
    """
    def __init__(self, db=config.background.jobtracker_db, autocommit=True):
        """Constructor for JobtrackerDatabase objects.
            
            Inputs:
                db: The database file to connect to. (Default: %s)
                autocommit: boolean, determines if autocommit should
                                be turned on or off.

            Output:
                A JobtrackerDatabase instance.
        """ % config.background.jobtracker_db
        self.attached_DBs = [] # databases that are attached.
        self.db = db
        self.connect(autocommit=autocommit)

    def connect(self, timeout=40, autocommit=True):
        """Establish a database connection. Self self.conn and self.cursor.
            
            NOTE: The database connected to is automatically attached as "jt".

            Inputs:
                timeout: Number of seconds to wait for a lock to be 
                    released before raising an exception. (Default: 40s)
                autocommit: boolean, determines if autocommit should
                                be turned on or off.

            Outputs:
                None
        """
        self.conn = sqlite3.connect(self.db, timeout=timeout)
        if autocommit:
            self.conn.isolation_level = None
        else:
            # Don't allow any other connections to read/write database during
            # a transaction.
            self.conn.isolation_level = 'EXCLUSIVE'
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.attach(self.db, 'jt')

    def attach(self, db, abbrev):
        """Attach another database to the connection.

            Inputs:
                db: The database file to attach.
                abbrev: The abbreviated name that should be used when
                    referring to the attached DB in SQL queries.

            Outputs:
                None
        """
        self.cursor.execute("ATTACH DATABASE ? AS ?", (db, abbrev))
        self.attached_DBs.append((db, abbrev))

    def show_attached(self):
        """Print all currently attached databases in the follwoing format:
            <database> AS <alias>

            Inputs:
                None
            
            Outputs:
                None
        """
        for attached in self.attached_DBs:
            print "%s AS %s" % attached

    def execute(self, query, *args, **kwargs):
        """Execute a single query.

            Inputs:
                query: The SQL query to execute.
                *NOTE: all other arguments are passed to the database
                    cursor's execute method.

            Outputs:
                None
        """
        if debug.JOBTRACKER:
            print query
        try:
            self.cursor.execute(query, *args, **kwargs)
        except sqlite3.OperationalError, e:
            sys.stderr.write("sqlite3.OperationError encountered. " \
                             "Rolling back.\n    %s" % str(e))
            self.conn.rollback()

    def commit(self):
        """Commit the currently open transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        self.conn.commit()

    def rollback(self):
        """Roll back the currently open transaction.

            Inputs:
                None

            Outputs:
                None
        """
        self.conn.rollback()

    def close(self):
        """Close the database connection.

            Inputs:
                None

            Outputs:
                None
        """
        self.conn.close()

    def fetchone(self):
        """Fetch a single row from the last executed query and return it.
            
            Inputs:
                None

            Output:
                row: The row pointed at by the DB cursor.
        """
        return self.cursor.fetchone()

    def fetchall(self):
        """Fetch all rows from the last executed query and return them.
            
            Inputs:
                None

            Output:
                rows: A list of rows pointed at by the DB cursor.
        """
        return self.cursor.fetchall()

    def showall(self):
        """Prettily show the rows currently pointed at by the DB cursor.

            Intputs:
                None

            Outputs:
                None
        """
        desc = self.cursor.description
        if desc is not None:
            fields = [d[0] for d in desc] 
            table = prettytable.PrettyTable(fields)
            for row in self.cursor:
                table.add_row(row)
            table.printt()
    
    def union(self, tablename):
        """Return a string that is the SQL syntax to return
            the union of 'tablename' for all attached databases.

            Input:
                tablename: The name of the table to unionize.

            Output:
                unionstr: The SQL string to perform the union.
        """
        return "(%s)" % " UNION ".join(["SELECT * FROM %s.%s" % \
                    (attached[1], tablename) 
                    for attached in self.attached_DBs])

    def copy(self, db_orig, db_dest, tablename, whereclause=None):
        """Copy rows in 'db_orig's 'tablename' to the corresponding
            table in 'db_dest'. All rows matching 'whereclause' are
            copied. Entire rows are copied.

            Inputs:
                db_orig: The alias of the database of origin.
                db_dest: The destination database's alias.
                tablename: The table where rows are being copied.
                whereclause: An optional where clause that determines
                    which rows are copied. They "WHERE" keyword should
                    be omitted. (Default: copy all rows).

            Outputs:
                None
        """
        query = "INSERT INTO %s.%s SELECT * FROM %s.%s" % \
                    (db_dest, tablename, db_orig, tablename)
        if whereclause is not None:
            query += " WHERE %s" % whereclause
        self.execute(query)

    def move(self, db_orig, db_dest, tablename, whereclause=None):
        """Move rows in 'db_orig's 'tablename' to the corresponding
            table in 'db_dest'. All rows matching 'whereclause' are
            moved.

            Inputs:
                db_orig: The alias of the database of origin.
                db_dest: The destination database's alias.
                tablename: The table where rows are being moved.
                whereclause: An optional where clause that determines
                    which rows are moved. They "WHERE" keyword should
                    be omitted. (Default: move all rows).

            Outputs:
                None

            NOTE: Moving is accomplished by first calling self.copy(...)
                and then deleting the rows from the origin database.
        """
        self.copy(db_orig, db_dest, tablename, whereclause)
        query = "DELETE FROM %s.%s" % (db_orig, tablename)
        if whereclause is not None:
            query += " WHERE %s" % whereclause
        self.execute(query)


def nowstr():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def query(queries, fetchone=False):
    """Execute multiple queries to the sqlite3 jobtracker database.
        All queries will be executed as a single transaction.
        Return the result of the last query, or the ID of the last
        INSERT, whichever is applicaple.

        Inputs:
            queries: A list of queries to be execute.
            fetchone: If True, fetch and return only a single row.
                        Otherwise, fetch and return all rows.
                        (Only applies for SELECT statements.
                        Default: fetch all rows).

        Outputs:
            results: Single row, or list of rows (for SELECT statements),
                        depending on 'fetchone'. Or, the ID of the last
                        entry INSERT'ed (for INSERT statements).
    """
    if isinstance(queries, (types.StringType, types.UnicodeType)):
        # Make a list if only a single string is pass in
        queries = [queries]
    not_connected = True
    count = 0
    while not_connected:
        try:
            db_conn = sqlite3.connect(config.background.jobtracker_db,timeout=40.0)
            db_conn.isolation_level = 'DEFERRED'
            db_conn.row_factory = sqlite3.Row
            db_cur = db_conn.cursor()
            for q in queries:
                if debug.JOBTRACKER:
                    print q
                db_cur.execute(q)
            db_conn.commit()
            if db_cur.lastrowid:
                results = db_cur.lastrowid
            else:
                if fetchone:
                    results = db_cur.fetchone()
                else:
                    results = db_cur.fetchall()
            db_conn.close()
            not_connected = False
        except sqlite3.OperationalError, e:
            try:
                db_conn.rollback()
                db_conn.close()
            except NameError:
                # Connection wasn't established, 'db_conn' is not defined.
                pass
            if (count % 60) == 0:
                if count > 1:
                    raise
                print "Couldn't connect to DB for %d seconds. Will continue trying. " \
                        "Error message: %s" % (count, str(e))
            time.sleep(1)
            count+=1
    return results


def execute(queries, arglists, fetchone=False):
    """Execute multiple queries to the sqlite3 jobtracker database.
        All queries will be executed as a single transaction.
        Return the result of the last query, or the ID of the last
        INSERT, whichever is applicaple.

        Inputs:
            queries: A list of queries to be execute.
            arglists: A list (same length as queries). 
                        Each entry contains the paramters to be
                        substituted into the corresponding query.
            fetchone: If True, fetch and return only a single row.
                        Otherwise, fetch and return all rows.
                        (Only applies for SELECT statements.
                        Default: fetch all rows).

        Outputs:
            results: Single row, or list of rows (for SELECT statements),
                        depending on 'fetchone'. Or, the ID of the last
                        entry INSERT'ed (for INSERT statements).
    """
    not_connected = True
    count = 0
    while not_connected:
        try:
            db_conn = sqlite3.connect(config.background.jobtracker_db,timeout=40.0)
            db_conn.isolation_level = 'DEFERRED'
            db_conn.row_factory = sqlite3.Row
            db_cur = db_conn.cursor()
            for q, args in zip(queries, arglists):
                db_cur.execute(q, args)
            db_conn.commit()
            if db_cur.lastrowid:
                results = db_cur.lastrowid
            else:
                if fetchone:
                    results = db_cur.fetchone()
                else:
                    results = db_cur.fetchall()
            db_conn.close()
            not_connected = False
        except sqlite3.OperationalError, e:
            try:
                db_conn.rollback()
                db_conn.close()
            except NameError:
                # Connection wasn't established, 'db_conn' is not defined.
                pass
            if (count % 60) == 0:
                print "Couldn't connect to DB for %d seconds. Will continue trying. " \
                        "Error message: %s" % (count, str(e))
            time.sleep(1)
            count+=1
    return results
