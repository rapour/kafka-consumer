import logging
import sys

class Logger:

    def __init__(self, name: str):
        
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        self.c_handler = logging.StreamHandler(sys.stdout)
        self.f_handler = logging.FileHandler(f"{name}.log")

        format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.c_handler.setLevel(logging.DEBUG)
        self.c_handler.setFormatter(format)
        
        self.f_handler.setLevel(logging.DEBUG)
        self.f_handler.setFormatter(format)

        self.logger.addHandler(self.c_handler)
        self.logger.addHandler(self.f_handler)

    def __del__(self):
        self.c_handler.close()
        self.f_handler.close()

    def getLogger(self):
        return self.logger
