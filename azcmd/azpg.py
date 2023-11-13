"""
Azure Storage Account utilities for use with postgres database backups.
"""
from pathlib import Path
from dataclasses import dataclass
import azcmd.funcs as azfunc
import socket
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

@dataclass
class AzPGBackup:
    storage_account: str = None
    container_name: str = None
    blob_path: str = None  # This is the path to the blob without the blob backup name
    backup_prefix:str  = None  # This is the prefix of the backup name. The backup name is the prefix + the timestamp
    db_name: str = None
    db_user: str = None
    db_password: str = None
    db_host: str = "localhost"


    def assert_storage_url(self, url):

        if not url.startswith("https://"):
            raise Exception(f"URL {url} does not start with https://")
        if not url.endswith("/"):
            url = url + "/"

        #remove the https:// and trailing / from the url
        domain_dns  = url[8:-1]

        #check that the domain_dns is in the form <storage_account>.blob.core.windows.net
        domain_parts = domain_dns.split(".")
        if len(domain_parts) != 5:
            raise Exception(f"URL {url} is not a valid storage account URL")

        #ensure that domain_dns resolves to an IP address
        try:
            socket.gethostbyname(domain_dns)
        except socket.gaierror:
            raise Exception(f"URL {url} does not resolve to an IP address. Maybe wrong storage account?")

    @property
    def account_url(self):
        return f"https://{self.storage_account}.blob.core.windows.net"

    @property
    def credential(self):
        return DefaultAzureCredential()

    def list_backups(self):
        """
        Get the latest backup from the blob path. This method users the storage_account , container name
        and blob path to list the blobs in the container and then finds the latest backup by sorting the
        names of the blob and picking the last one.
        """

        blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.credential)
        container_client = blob_service_client.get_container_client(self.container_name)
        blob_list = container_client.list_blobs()
        latest_backup = None

        if self.blob_path: # If the blob path is specified, then filter the blobs to only those that start with the blob path
            blob_list = [blob for blob in blob_list if blob.name.startswith(self.blob_path)]
            return blob_list
        else:
            raise Exception(f"No backups found.")


    def get_latest_backup(self):
        """
        Get the latest backup from the blob path. This method users the storage_account , container name
        and blob path to list the blobs in the container and then finds the latest backup by sorting the
        names of the blob and picking the last one.
        """

        blob_names = self.list_backups()
        blob_names.sort(key=lambda blob_name: blob_name.name)

        if len(blob_names) > 0:
            return blob_names[-1]
        else:
            print(f"No backups found in container {self.container_name} with path {self.blob_path}")

    def download_latest_backup(self, path=None  ):
        """
        Download the latest backup from the blob path. This method uses the storage_account , container name
        :return:
        """

        latest_backup = self.get_latest_backup()
        if latest_backup:
            blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.credential)
            container_client = blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(latest_backup)
            if path:
                path = Path(path)
                path.mkdir(parents=True, exist_ok=True)
                download_path = path / Path(latest_backup.name).name
            else:
                download_path = Path(latest_backup.name).name

            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            print(f"Downloaded backup to {download_path}")
        else:
            print(f"No backups found in container {self.container_name} with path {self.blob_path}")

def main():
    """
    Provides command line interface to the azpgbackup class
    :return:
    """

    parser = argparse.ArgumentParser(description='Download latest postgres backup from Azure Blob Storage')
    parser.add_argument('storage_account', type=str, help='Storage account URL')
    parser.add_argument('container_name', type=str, help='Container name')
    #blob_path is optional
    parser.add_argument('blob_path', type=str, help='Blob path', nargs='?', default=None)
    parser.add_argument('backup_prefix', type=str, help='Backup prefix', nargs='?', default=None)

    pg_backups = AzPGBackup()
    args = parser.parse_args()
    pg_backups.storage_account = args.storage_account
    pg_backups.container_name = args.container_name
    pg_backups.blob_path = args.blob_path
    pg_backups.backup_prefix = args.backup_prefix
    pg_backups.download_latest_backup()


if __name__ == "__main__":

    main()





