import argparse
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pathlib import Path
from dataclasses import dataclass
def _download_blob(storage_account, container_name, blob_name, downloaded_file_path=None):
    # Define your Azure Blob Storage account URL
    account_url = f"https://{storage_account}.blob.core.windows.net"

    print(f"account_url={account_url}")
    print(f"contanier_name={container_name}")
    print(f"blob_name={blob_name}")

    # Create an instance of DefaultAzureCredential for authentication
    credential = DefaultAzureCredential()

    print("Conencting to blob")
    # Create a BlobServiceClient using the account URL and managed identity credential
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

    print("Getting blob client")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)


    path = Path(blob_name)
    path.parent.mkdir(parents=True, exist_ok=True)

    print("Downloading blob to " + str(path) + "...", end="")
    if downloaded_file_path is None:
        downloaded_file_path = path.name

    with open(downloaded_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    print("Done")



def azget(source_path, destination=None):
    """
    Download a file or directory from Azure Blob Storage to the local filesystem.

    source_path: The azure storage account path that you want to download. This starts with
    the storage_account name and then the container name, and then the path to the file.

    example: storeage_account/container_name/path/to/file.txt
    """
    blob_info  =  BlobInfo().from_path(source_path)

    print(f"account_url={blob_info.url}")
    print(f"contanier_name={blob_info.container_name}")
    print(f"blob_name={blob_info.blob_name}")

    _download_blob(blob_info.storage_account, blob_info.container_name, blob_info.blob_name)


if __name__ == "__main__":
  # Just something to test this script with


    azget("storbackupscramoprod0/cramo-test/testfile.txt")


