import os
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


class AzureBlobStorage:
    """
    A class to interact with Azure Blob Storage
    """

    def __init__(self):
        load_dotenv()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        print(self.connection_string)
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def create_container(self, container_name):
        """Creates a container in Azure Blob Storage.   

        Args:
            container_name (str): The name of the container to create

        Returns:
            object: ContainerClient
        """
        return self.blob_service_client.create_container(container_name)

    def insert_data(self, data, container_client):
        """Inserts data into Azure Blob Storage.

        Args:
            data (dict): The data to insert
            container_client (ContainerClient): The ContainerClient object
        """
        blob_name = f"{data['product_id']}_{data['time']}.json"
        blob_client = container_client.get_blob_client(blob_name)

        data_str = json.dumps(data)
        data_bytes = data_str.encode("utf-8")

        blob_client.upload_blob(data_bytes)
    