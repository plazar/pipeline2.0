
import os.path
import sys
import re
from astro_utils.database import Database
import config

dmhit_re = re.compile(r'^ *DM= *(?P<dm>[^ ]*) *SNR= *(?P<snr>[^ ]*) *\** *$')
candinfo_re = re.compile(r'^(?P<accelfile>.*):(?P<candnum>\d*) *(?P<dm>[^ ]*)' \
                         r' *(?P<snr>[^ ]*) *(?P<sigma>[^ ]*) *(?P<numharm>[^ ]*)' \
                         r' *(?P<ipow>[^ ]*) *(?P<cpow>[^ ]*) *(?P<period>[^ ]*)' \
                         r' *(?P<r>[^ ]*) *(?P<z>[^ ]*) *\((?P<numhits>\d*)\)$')


class CandUploader(object):

    def __init__(self,presto_jobs_output_dir,db_name=None,header_id=None):
        candidates_file=None
        self.cands=[]
        self.header_id = header_id
        if os.path.exists(presto_jobs_output_dir):
            for fname in os.listdir(presto_jobs_output_dir):
                if os.path.isfile(presto_jobs_output_dir +"/"+ fname) and len(fname) > 11:
                    if fname[len(fname)-11:] == '.accelcands':
                        candidates_file = presto_jobs_output_dir +"/"+ fname
                        break


        # Connect to DB
        if not db_name:
            self.db = Database()
        else:
            self.db = Database(db_name)

        if not candidates_file:
            raise Exception("Candidates file was not found in: '%s'" % presto_jobs_output_dir)

        self.file = open(candidates_file,'r')

    def upload(self):
        self.parse_file()
        if len(self.cands) == 0: #Nothing to upload
            exit(0)

        for cand in self.cands:
            self.insert_cand(cand)

    def insert_cand(self, candidate):
        #@institution = config.institution
        #@pipeline = config.pipeline
        #@version =
        query = "EXEC spPDMCandUploaderFindsVersion " + \
            "@header_id = %i, " % self.header_id + \
            "@cand_num = %i, " % candidate['candnum'] + \
            "@frequency = 12.5, " + \
            "@period = %f, " % candidate['period'] + \
            "@dm = %f, " % candidate['dm'] + \
            "@snr = %f, " % candidate['snr'] + \
            "@num_harmonics = %i, " % candidate['numharm'] + \
            "@institution = '%s', " % config.institution + \
            "@pipeline = '%s', " % config.pipeline + \
            "@version_number = '1.15.2', " + \
            "@proc_date = '10/31/2007 "
        self.db.cursor.execute(query)

        #self.db.commit()
        result = self.db.cursor.fetchone()
        for row in result:
            print result

    def parse_file(self):
        self.cands=[]
        for line in self.file:
            if not line.partition("#")[0].strip():
                # Ignore lines with no content
                continue
            dmhit_match = dmhit_re.match(line)
            candinfo_match = candinfo_re.match(line)
            if dmhit_match is not None:
                print dmhit_match.groupdict()
            elif candinfo_match is not None:
                print candinfo_match.groupdict()
                self.cands.append(candinfo_match.groupdict())
            else:
                sys.stderr.write("Line has unrecognized format!\n(%s)\n" % line)
                sys.exit(1)

if __name__=='__main__':
    upler=CandUploader(sys.argv[1])
    upler.parse_file()
    print "Total# of Candidates: %s ." % len(upler.cands)