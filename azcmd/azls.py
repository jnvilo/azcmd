from azcmd.models import BlobInfo
import argparse
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pathlib import Path

def azls(blob_storage_path):
    """
    List the blobs in a container
    """
    blob_info = BlobInfo().from_path(blob_storage_path)

    credential = DefaultAzureCredential()
    account_url = f"https://{blob_info.storage_account}.blob.core.windows.net"


    try:
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        container_client = blob_service_client.get_container_client(blob_info.container_name)
        blob_list = container_client.list_blobs()

        for blob in blob_list:
            if blob.name.startswith(blob_info.blob_name):
                print(blob.name)

    except Exception as e:
        if e.error_code == "ContainerNotFound":
            print(f"Container {blob_info.container_name} does not exist in storage account {blob_info.storage_account}")
        else:
            print("Unhandled exception:")
            print(e.message)


def main():
    parser = argparse.ArgumentParser(description='List blobs in a container')
    parser.add_argument('blob_storage_path', type=str, help='Blob storage path')
    args = parser.parse_args()
    azls(args.blob_storage_path)

def test_main():
    azls("/storbackupscramoprod0/cramo-test/db")


