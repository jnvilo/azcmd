from azcmd.models import BlobInfo
import argparse
from dataclasses import dataclass
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pathlib import Path
from azcmd import funcs as azfunc

@dataclass
class StorageAccountAccessErrorInfo:
    message: str = None
    storage_account_name: str = None

    def __str__(self):
        return f"StorageAccountAccessErrorInfo: {self.storage_account_name}\n {self.message}"

def get_storage_account_names():
    """
    Gets a list of storage account names for all subscriptions.
    :return:
    """
    storage_accounts = azfunc.get_storage_accounts()
    return [account.name for account in storage_accounts]

def get_container_paths(storage_account=None):
    """
    Gets a list of storage containers for a storage account. If storage_account is not provided then
    it gets all storage accounts for all subscriptions.
    :param storage_account:
    :return:
    """
    error_list = []

    if storage_account is None:
        storage_accounts = azfunc.get_storage_accounts()
    else:
        storage_accounts = [storage_account]

    container_names = []
    for account in storage_accounts:
        try:
            credential = DefaultAzureCredential()
            account_url = f"https://{account.name}.blob.core.windows.net"
            print(account_url)
            blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
            containers =  blob_service_client.list_containers()

            for container in containers:
                container_names.append(f"{account.name}/{container.name}")
        except Exception as e:
            error_list.append(StorageAccountAccessErrorInfo(e.message, account.name))

    for error in error_list:
        print(error)

    return container_names





