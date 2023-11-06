from models import BlobInfo
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

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(blob_info.container_name)
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print(blob.name)

def main():
    parser = argparse.ArgumentParser(description='List blobs in a container')
    parser.add_argument('blob_storage_path', type=str, help='Blob storage path')
    args = parser.parse_args()
    azls(args.blob_storage_path)

def test_main():
    azls("/storbackupscramoprod0/cramo-test/db")


test_main()