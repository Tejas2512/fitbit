from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from io import StringIO
from load_yaml import yaml_ops
import os
import pickle

# pip install azure-storage-blob

class azure_methodes:

    def __init__(self, logger_object, file_object):
        self.yaml = yaml_ops()
        self.config = self.yaml.load_file(os.path.join("configfile","connection.yaml"))
        self.account_name = self.config["azure"]["account_name"]
        self.logger_object = logger_object
        self.file_object = file_object
        self.connection_string = self.config["azure"]["connection_string"]

    def connection(self):

        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            #self.logger_object.log(self.file_object,"connection establish..")
            return blob_service_client

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred in azure connection.".format(e))
            raise e

    def create_container(self,container_name):

        try:
            blob_service_client = self.connection()

            container_client = blob_service_client.create_container(container_name)
            self.logger_object.log(self.file_object,"{} container created.".format(container_name))

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred while create {} container.".format(e,container_name))

    def get_all_blob(self,container_name):

        try:
            blob_service_client = ContainerClient.from_connection_string(conn_str=self.connection_string,container_name=container_name)

            onlyfiles = [x.name for x in blob_service_client.list_blobs()]
            self.logger_object.log(self.file_object,"Successfully list all blobs..")
            return onlyfiles

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred while listing all blobs.".format(e))
            raise e

    def copy_blob(self,source_container,source_blob, destination_container, blob_name):

        try:
            blob_service_client = self.connection()
            source_blob = (f"https://{self.account_name}.blob.core.windows.net/{source_container}/{source_blob}")

            # Target
            copied_blob = blob_service_client.get_blob_client(destination_container, blob_name)
            copied_blob.start_copy_from_url(source_blob)

            self.logger_object.log(self.file_object,"Successfully copy blob from {} to {}.".format(source_container,destination_container))

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred while copying blob from {} to {}.".format(e,source_container,destination_container))
            raise e

    def move_blob(self, source_container, destination_container, blob_name=None, copy_type="all"):

        try:
            blob_service_client = self.connection()
            if copy_type != "all":
                source_blob = (f"https://{self.account_name}.blob.core.windows.net/{source_container}/{blob_name}")

                # Target
                copied_blob = blob_service_client.get_blob_client(destination_container, blob_name)
                copied_blob.start_copy_from_url(source_blob)

                # If you would like to delete the source file
                remove_blob = blob_service_client.get_blob_client(source_container, blob_name)
                remove_blob.delete_blob()

            else:
                onlyfiles = self.get_all_blob(container_name=source_container)
                for file in onlyfiles:
                    source_blob = (f"https://{self.account_name}.blob.core.windows.net/{source_container}/{file}")

                    # Target
                    copied_blob = blob_service_client.get_blob_client(destination_container, file)
                    copied_blob.start_copy_from_url(source_blob)

                    # If you would like to delete the source file
                    remove_blob = blob_service_client.get_blob_client(source_container, file)
                    remove_blob.delete_blob()

            self.logger_object.log(self.file_object,"Successfully move all blobs from {} to {}.".format(source_container,destination_container))

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred while moving all blobs from {} to {}.".format(e,source_container,destination_container))
            raise e

    def delete_blob(self,container_name, blob_name =None, type_="all"):

        try:
            blob_service_client = self.connection()

            if type_ != str.lower("all"):
                remove_blob = blob_service_client.get_blob_client(container_name, blob_name)
                remove_blob.delete_blob()

            else:
                onlyfiles = self.get_all_blob(container_name=container_name)

                for file in onlyfiles:
                    remove_blob = blob_service_client.get_blob_client(container_name, file)
                    remove_blob.delete_blob()

            self.logger_object.log(self.file_object,"Successfully delete all blobs of {} container.".format(container_name))

        except Exception as e:
            self.logger_object.log(self.file_object, "Error:: {} occurred while deleting blobs from {}.".format(e,container_name))
            raise e

    def read_blob(self, container_name, blob_name):

        try:
            container_client = ContainerClient.from_connection_string(conn_str=self.connection_string, container_name=container_name)
            # Download blob as StorageStreamDownloader object (stored in memory)
            downloaded_blob = container_client.download_blob(blob_name)##("InputFile.csv")

            df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))

            self.logger_object.log(self.file_object,"Successfully read blobs of {} container.".format(container_name))

            return df

        except Exception as e:
            self.logger_object.log(self.file_object,"Error:: {} occurred while reading blobs from {}.".format(e,container_name))
            raise e

    def read_pickel(self, container_name, blob_name):

        try:
            blob_client = BlobClient.from_connection_string(self.connection_string, container_name, blob_name)
            downloader = blob_client.download_blob(0)

            # Load to pickle
            b = downloader.readall()
            model = pickle.loads(b)

            self.logger_object.log(self.file_object,"Successfully read blobs of {} container.".format(container_name))
            return model

        except Exception as e:
            self.logger_object.log(self.file_object,"Error:: {} occurred while reading blobs from {}.".format(e,container_name))
            raise e

    def upload_blob(self,data,container_name,blob_name):

        try:
            blob = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=container_name,
                                                     blob_name=blob_name)

            blob.upload_blob(data,overwrite=True)

            self.logger_object.log(self.file_object,"Blob:: {} uploaded successfully.".format(blob_name))

        except Exception as e:

            self.logger_object.log(self.file_object,"Error:: {} occurred while uploading blob.".format(e))
            raise e