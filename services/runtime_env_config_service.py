import os
from configparser import ConfigParser

def read_configs(env=os.getenv("ENVIRONMENT","PRODUCTION")):
    config_parser = ConfigParser()
    if env=="PRODUCTION":        
        config_parser.read("configs/prod_config.cfg")
    else:
        config_parser.read("configs/dev_config.cfg")
    return config_parser

