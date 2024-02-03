import os
from mysql import connector
from services.runtime_env_config_service import read_configs
from services.logger_service import logger
import traceback


configs = read_configs()
HOST = configs["SQLDB"]["sql.host"]
PORT = configs["SQLDB"]["sql.port"]
USER = configs["SQLDB"]["sql.user"]
PASSWORD = configs["SQLDB"]["sql.password"]
DATABASE = configs["SQLDB"]["sql.database"]
DRIVER = configs["SQLDB"]["sql.driver"]


class DbOps:
    def __init__(self, host:str=HOST, port:int=PORT, user:str=USER, password=PASSWORD, database=DATABASE):
        try:
            self.client = connector.connect(host=host, port=port, user=user, password=password, db=database )
            self.cursor = self.client.cursor(dictionary=True)
            logger.info("Successfully connected with SQL server")
        except:
            error = f"Failed to connected with SQL server.\nError: {traceback.format_exc()}"
            logger.error(error)
            raise Exception(error)       


    def get_client(self):
        return self.client
    

    def execute_command(self,command:str):
        error, output = "",""
        try:
            self.cursor.execute(command)
            output = self.cursor.fetchall()
            self.client.commit()
            logger.info(f"Successfully executed the command:\n``{command}``")            
        except Exception as e:
            error = f"Failed to execute the command due to error: {traceback.format_exc()}"
            logger.error(error)
            
        return output, error


    def close_session(self):
        self.client.commit()
        self.client.disconnect()
        logger.info("Successfully closed the connection with SQL server")

