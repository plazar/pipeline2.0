import pyodbc

class Database:
    
    def __init__(self, db="palfa-common-copy"):
        #self.conn_original = self.connect("palfa-common")
        #self.conn_copy = self.connect("palfa-common-copy")
        self.conn = self.connect(db)
        self.cursor = self.conn.cursor()
    
    def connect(self, db="palfa-common-copy"):
        #conn = pyodbc.connect("DRIVER={SQL Native Client};SERVER=arecibosql.tc.cornell.edu;UID=sequenceRafal;PWD=53qu3Nc3r@faL;DATABASE=" + db)
        conn = pyodbc.connect("textsize=600000;DSN=FreeTDSDSN;UID=sequenceRafal;PWD=53qu3Nc3r@faL;DATABASE=" + db)
        return conn

    def commit(self):
        self.conn.commit()
    
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
    
