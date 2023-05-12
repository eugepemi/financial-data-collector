import os
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import uuid


class AzureBlobStorage:
    """
    A class to interact with Azure Blob Storage
    """

    def __init__(self):
        load_dotenv()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def create_container(self, container_name):
        """Creates a container in Azure Blob Storage.   

        Args:
            container_name (str): The name of the container to create

        Returns:
            object: ContainerClient
        """
        return self.blob_service_client.create_container(container_name)

    def insert_data(self, data, container_name):
        """Inserts data into Azure Blob Storage.

        Args:
            data (dict): The data to insert
            container_name (str): The name of the container
        """
        current_time = datetime.now().isoformat()
        blob_name = f"{container_name}_{current_time}_{uuid.uuid4()}.json"
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        data_str = json.dumps(data)
        data_bytes = data_str.encode("utf-8")
        
        blob_client.upload_blob(data_bytes)
        print('Loaded on storage')
    