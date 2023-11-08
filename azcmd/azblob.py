"""
Implements the command line interface to azblob
"""
import argparse
from azcmd import funcs as azfunc
from azcmd.models import BlobInfo
from pathlib import Path

class CommandRegistryBase(type):

    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        # instantiate a new type corresponding to the type of class being defined
        # this is currently RegisterBase but in child classes will be the child class
        new_cls = type.__new__(cls, name, bases, attrs)
        cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


class CommandRegistry(metaclass=CommandRegistryBase):

    @classmethod
    def get_command(cls, command_name):
        return cls.REGISTRY[command_name]()

class Command(CommandRegistry):
    pass


class LsCommand(Command):

    def list_all_storage_accounts(self):
        # Should list all the storage accounts
        storage_accounts = azfunc.get_storage_account_names()
        for storage_account in storage_accounts:
            print(storage_account)

    def list_storage_account_containers(self, storage_account):
        # Should list all the containers in a storage account
        containers = azfunc.get_containers(storage_account)
        for container in containers:
            container_path = f"/{storage_account}/{container.name}"
            print(container_path)

    def list_storage_account_container_blobs(self, storage_account, container):
        # Should list all the blobs in a container
        blobs = azfunc.get_container_blobs(storage_account, container)


        #for blob in blobs:
        #    blob_path = f"/{storage_account}/{container}/{blob.name}"
        #    print(blob_path)
        blob_dirs = []
        for blob in blobs:
            blob_name = blob.name.split('/')[0]
            if blob_name not in blob_dirs:
                blob_dirs.append(blob_name)
                print(blob_name)



    def execute(self, args):
        """List blobs in a container"""

        storage_path = args.storage_path
        if storage_path is None:
            self.list_all_storage_accounts()
        else:
            if storage_path.startswith("/"):
                storage_path = storage_path[1:]
            if storage_path.endswith("/"):
                storage_path = storage_path[:-1]
            storage_path_parts = storage_path.split('/')

            if len(storage_path_parts) == 1:
                self.list_storage_account_containers(storage_path_parts[0])

            elif len(storage_path_parts) == 2:
                self.list_storage_account_container_blobs(storage_path_parts[0], storage_path_parts[1])

            elif len(storage_path_parts) > 2:
                #Here we have a blob path and we need to list the blobs only in this path.
                blob_parts = storage_path_parts[2:]
                blob_path = "/".join(blob_parts)
                blobs = azfunc.get_container_blobs(storage_path_parts[0], storage_path_parts[1])

                filtered_blobs = []
                for blob in blobs:
                    if blob.name.startswith(blob_path):
                        filtered_blobs.append(blob)
                        print(blob.name.lstrip(blob_path).lstrip("/"))
                return filtered_blobs

class GetCommand(Command):
    def execute(self, args):
        storage_path = args.storage_path

        blob_info  =  BlobInfo().from_path(storage_path)

        # first check if the storage_account is valid
        stor = azfunc.get_storage_account_by_name(blob_info.storage_account)
        if stor is None:
            print(f"Storage account {blob_info.storage_account} not found")
            return

        if blob_info.has_blob:
            azfunc.download_blob(storage_path)

        elif blob_info.has_container:
            # No blob listed, we want to get the whole container

            blobs = azfunc.get_container_blobs(blob_info.storage_account, blob_info.container_name)
            for blob in blobs:
                blob_path = f"/{blob_info.storage_account}/{blob_info.container_name}/{blob.name}"
                print(blob_path)


class PutCommand(Command):

    def execute(self, args):
        """
        Upload baackups to their respective containers
        """
        source = args.source_path
        destination = args.destination_path #This is the <storage_account><container><blob> path

        source = Path(source)

        if source.is_dir():
            #upload all the files in the directory
            for file in source.glob("**/*"):
                if file.is_file():
                    destination_path = destination + "/" + str(file.relative_to(source))
                    azfunc.upload_blob(str(file), destination_path)
        elif source.is_file():
            azfunc.upload_blob(str(source), destination, overwrite=args.overwrite)

def main():

    #create an argument parser
    parser = argparse.ArgumentParser(description='azblob command line interface')

    #create a subparser for the ls command
    subparsers = parser.add_subparsers(help='sub-command help', dest='command')
    parser_ls = subparsers.add_parser('ls', help='list blobs in a container')

    #create the storage_path argument , can be optional
    parser_ls.add_argument('storage_path', type=str, help='Blob Path', nargs='?', default=None)

    parser_get = subparsers.add_parser('get', help='get blob from a container')
    parser_get.add_argument('storage_path', type=str, help='Blob Path')

    parser_put = subparsers.add_parser('put', help='Upload blob/s to a container')
    parser_put.add_argument('source_path', type=str, help='Source Path')
    parser_put.add_argument('destination_path', type=str, help='Destination Path')
    #add flag --overwrite to overwrite existing blobs
    parser_put.add_argument('--overwrite', action='store_true', help='Overwrite existing blobs')


    #Display the arguments passed
    args = parser.parse_args()
    command = args.command

    if command is not None:

        command_class_name = f"{command.capitalize()}Command"
        command_class = Command.REGISTRY[command_class_name]()
        command_class.execute(args)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()