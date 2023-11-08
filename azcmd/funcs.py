from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient
from azure.storage.blob import BlobServiceClient
from azure.mgmt.resource import ResourceManagementClient
from dataclasses import dataclass
from azcmd.models import BlobInfo
from pathlib import Path
import sys

@dataclass
class StorageAccountAccessErrorInfo:
    message: str = None
    storage_account_name: str = None

    def __str__(self):
        return f"StorageAccountAccessErrorInfo: {self.storage_account_name}\n {self.message}"

def get_subscriptions():
    """Returns a list of subscriptions"""
    credential = DefaultAzureCredential()
    subscription_client = SubscriptionClient(credential)
    subscriptions = []
    for sub in subscription_client.subscriptions.list():
        subscriptions.append(sub)
    return subscriptions

def get_subscription_ids():
    """Returns a list of subscription ids"""
    subscriptions = get_subscriptions()
    subscription_ids = []
    for sub in subscriptions:
        subscription_ids.append(sub.subscription_id)
    return subscription_ids


def get_storage_accounts():
    """Returns a list of storage accounts"""
    subscription_ids = get_subscription_ids()
    storage_accounts = []
    for subscription_id in subscription_ids:
        credential = DefaultAzureCredential()
        resource_client = ResourceManagementClient(credential, subscription_id)
        for account in resource_client.resources.list(filter="resourceType eq 'Microsoft.Storage/storageAccounts'"):
            storage_accounts.append(account)
    return storage_accounts

def get_storage_account_ids():
    """Returns a list of storage account ids"""
    storage_accounts = get_storage_accounts()
    storage_account_ids = []
    for account in storage_accounts:
        storage_account_ids.append(account.id)
    return storage_account_ids

def get_storage_account_names():
    """REturns alist of storage account names"""
    storage_accounts = get_storage_accounts()
    storage_account_names = []
    for account in storage_accounts:
        storage_account_names.append(account.name)
    return storage_account_names

def get_storage_account_by_name(storage_account_name):
    """Returns a storage account by name"""
    storage_accounts = get_storage_accounts()
    for account in storage_accounts:
        if account.name == storage_account_name:
            return account
    return None

def get_containers(storage_account: str):
    """
    Gets a list of storage containers for a storage account. If storage_account is not provided then
    it gets all storage accounts for all subscriptions.
    :param storage_account:
    :return:
    """

    credential = DefaultAzureCredential()
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    containers =  blob_service_client.list_containers()
    return containers


def get_container_blobs(storage_account, container_name):
    """
    Gets a list of blobs in a container.
    :param storage_account:
    :param container_name:
    :return:
    """


    credential = DefaultAzureCredential()
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    return blob_list

def get_blob_list(storage_account, container_name, blob_path):
    """
    Gets a list of blobs in a container.
    :param storage_account:
    :param container_name:
    :return:
    """

    credential = DefaultAzureCredential()
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()

    parent_path = Path(blob_path)
    filtered_blobs = []
    #We have to filter the blob_list to return only the ones that start with blob_path
    for blob in blob_list:
        child_path = Path(blob.name)
        if parent_path in child_path.parents:
            filtered_blobs.append(blob)

    return filtered_blobs

def download_blob(storage_path, destination=None):
    """
    Downloads a blob to the current directory or to the destination directory if provided.
    :param storage_path:
    :return:
    """

    blob_info  =  BlobInfo().from_path(storage_path)

    print(f"account_url={blob_info.url}")
    print(f"contanier_name={blob_info.container_name}")
    print(f"blob_name={blob_info.blob_name}")

    account_url = f"https://{blob_info.storage_account}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

    #the blob_name is the path to the file on the blob storage account. But it could also be a virtual directory.
    #find out first if it is a virtual directory. If it is then we need to create the directory on the local fi

    blob_name = blob_info.blob_name
    from pprint import pprint
    blobs = get_container_blobs(blob_info.storage_account, blob_info.container_name)


    """ 
    Figure out if the blob_name is a virtual directory or a file.
    Azure Blob Storage does not have a concept of a directory. It is just a flat namespace,
    """

    # make a list of blob names
    blob_names = []
    for blob in blobs:
        blob_names.append(blob.name)

    # find out if the blob_name is a virtual directory
    if blob_name in blob_names:

        if destination and destination.endswith("/"):
            """
                If the destination ends with a / , then it means
                it is a directory. Therefore we should append the blob_name to the destination
            """
            Path(destination).mkdir(parents=True, exist_ok=True)
            destination = Path(destination, Path(blob_name).name)

        elif destination and not destination.endswith("/"):
            #If the destination does not end with a /, then it means
            #it is a file. Therefore we should use the destination as is
            destination = Path(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)


        with open(destination, "wb") as f:
            f.write(blob_service_client.get_blob_client(blob_info.container_name, blob_name).download_blob().readall())
    else:
        #WE have to check that there is a blob that starts with the blob_name
        #collect all  the blobs that start with the blob_name
        filtered_blobs = []

        for bname in blob_names:
            if bname.startswith(blob_name):
                filtered_blobs.append(bname)

        for blob_name in filtered_blobs:
            path = Path(blob_name)
            path.parent.mkdir(parents=True, exist_ok=True)
            print(f"Downloading blob: {blob_name}")
            print(f"Saving to: {path.name}")

            with open(path, "wb") as f:
                f.write(blob_service_client.get_blob_client(blob_info.container_name, blob_name).download_blob().readall())
            print("Downloaded blob: {}".format(blob_name))



def upload_blob(source_path, destination_path, overwrite=False):

    #The destination path is the whole path to the blob including the storage account, container and blob name

    print(destination_path)
    import sys

    blob_info = BlobInfo().from_path(destination_path)
    blob_name = blob_info.blob_name
    from pprint import pprint

    blob_service_client = BlobServiceClient(account_url=blob_info.account_url, credential=DefaultAzureCredential())

    with open(source_path, "rb") as data:
        print(f"Uploading blob: {blob_info.blob_name}")
        blob_client = blob_service_client.get_blob_client(container=blob_info.container_name,
                                                          blob=blob_info.blob_name)
        if blob_client.exists() and overwrite is False:
            print(f"Blob {blob_name} already exists. Use --overwrite to overwrite it.")
            return
        else:
            blob_client.upload_blob(data, overwrite=overwrite)


def verify_storage_account_exists(storage_account):
    """
    Verifies that a storage account exists
    :param storage_account:
    :return:
    """
    stor = get_storage_account_by_name(storage_account)
    if stor is None:
        return False
    else:
        return True

def verify_container_exists(storage_account, container_name):
    """
    Verifies that a container exists
    :param storage_account:
    :param container_name:
    :return:
    """
    containers = get_containers(storage_account)
    for container in containers:
        if container.name == container_name:
            return True
    return False


def assert_storage_account_exists(storage_account):
    """
    Asserts that a storage account exists
    :param storage_account:
    :return:
    """
    if not verify_storage_account_exists(storage_account):
        print(f"Storage account {storage_account} does not exist")
        return False
    else:
        return True


def assert_container_exists(storage_account, container_name):
    """
    Asserts that a container exists
    :param storage_account:
    :param container_name:
    :return:
    """
    if not verify_container_exists(storage_account, container_name):
        print(f"Container {container_name} does not exist")
        return False
    else:
        return True

