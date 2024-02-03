from google.cloud import storage
import traceback
from services.runtime_env_config_service import read_configs
from services.logger_service import logger


configs = read_configs()
CREDENTIALS_FILE = configs["AUTH"]["gcp_creds_json"]
BUCKET = configs["STORAGE"]["cloud.bucket_name"]
INPUT_FILES_FOLDER = configs["STORAGE"]["cloud.input_data"]

class BucketOps:
    def __init__(self, json_credentials_path:str=CREDENTIALS_FILE):
        try:
            self.bucket_client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
            logger.info("Successfully created Cloud bucket client")
        except Exception:
            error = f"Failed to created Cloud bucket client.\nError: {traceback.format_exc()}"
            logger.error(error)
            raise Exception(error)

    def get_client(self):
        return self.bucket_client
    
    
    def list_files(self, bucket:str=BUCKET, folder_path:str=INPUT_FILES_FOLDER):
        error, files = "",[]
        try:
            logger.info(f"Looking for files in Bucket at location: {bucket}/{folder_path}/")
            response = self.bucket_client.list_blobs(bucket_or_name=bucket, prefix=folder_path)
            files = [blob.name for blob in response if blob.name!=folder_path+"/"]
        except Exception as e:
            error = f"Failed to fetch available files list from bucket {bucket}\nError: {traceback.format_exc()}"
            logger.error(error)
        print(files)
        
        return files,error
    

    def move_file(self, file_to_move:str, folder_path:str, bucket:str=BUCKET):
        bucket = self.bucket_client.bucket(bucket_name=bucket)
        error = ""
        try:            
            filename = file_to_move.rsplit("/")[-1]
            destination_object = folder_path+"/"+filename

            source_blob = bucket.blob(blob_name=file_to_move)
            destincation_blob = bucket.copy_blob(source_blob, bucket, destination_object)
            bucket.delete_blob(file_to_move)
            logger.info(f"Successfully moved the file from {source_blob.name} to location: {destincation_blob.name}")
        except:
            error = f"Failed to move the file from {file_to_move} to {destination_object}\nError: {traceback.format_exc()}"
            logger.error(error)

        return error



