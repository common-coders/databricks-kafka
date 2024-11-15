from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# Initialize the Blob Service Client
def initialize_blob_service(connection_string):
    return BlobServiceClient.from_connection_string(connection_string)

# Function to create a new container
def create_container(blob_service_client, container_name):
    try:
        container_client = blob_service_client.create_container(container_name)
        print(f'Container `{container_name}` created successfully.')
        return container_client
    except ResourceExistsError:
        print(f'Container `{container_name}` already exists.')


# Function to delete a container
def delete_container(blob_server_client, container_name):
    try:
        blob_server_client.delete_container(container_name)
        print(f'Container `{container_name}` is successfully deleted.')
    except ResourceNotFoundError:
        print(f'Container `{container_name}` does not exist.')

# Function for upload a blob to the container
def upload_blob(container_client, blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite = True)
    print(f'Blob `{blob_name}` uploaded successfully.')


# Function to downnload a blob from a container
def download_blob(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    try:
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        print(f'Blob `{blob_name}` downloaded successfully.')
        return data 
    except ResourceNotFoundError:
        print(f'Blob `{blob_name}` does not exist.')


#Function to list all blobs in a container
def list_blob(container_client):
    blobs = container_client.list_blobs()
    blob_names = [blob.name for blob in blobs]
    print(f'Blobs in the container:', blob_names)
    return blob_names

# Function to list all the containers in the storage account
def list_containers(blob_service_client):
    try:
        containers = blob_service_client.list_containers()
        print(f'Containers in the storage account:')
        for container in containers:
            print(f"- {container['name']}")
    except Exception as e:
        print("Error occurred while listing containers:", e)

