import logging

class OutStream:
    
    INFO = 1
    WARNING = 2
    DEBUG = 3
    ERROR = 4
    CRITICAL = 5
    
    def __init__(self,module_name_str, log_filename_str, screen_out_bool = True):
        self.screen_out=screen_out_bool
        self.log_fn=log_filename_str
        self.logger=logging.getLogger(module_name_str)
        self.logger.setLevel(logging.DEBUG)
        formatter= logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        logfile_h=logging.FileHandler(log_filename_str)
        logfile_h.setFormatter(formatter)
        self.logger.addHandler(logfile_h)
        if screen_out_bool:
            console_h = logging.StreamHandler()
            console_h.setFormatter(formatter)
            self.logger.addHandler(console_h)
        
    def outs(self, str_to_output,type=INFO):
        if type==OutStream.INFO:
            self.logger.info(str_to_output)
        elif type==OutStream.WARNING:
            self.logger.warning(str_to_output)
        elif type==OutStream.DEBUG:
            self.logger.debug(str_to_output)
        elif type==OutStream.ERROR:
            self.logger.error(str_to_output)
        elif type==OutStream.CRITICAL:
            self.logger.critical(str_to_output)