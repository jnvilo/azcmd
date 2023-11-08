"""
Azure Storage Account utilities for use with postgres database backups.
"""
from pathlib import Path
from dataclasses import dataclass
import azcmd.funcs as azfunc

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

    def get_latest_backup(self):
        """
        Get the latest backup from the blob path
        """

        # get a list of all the blobs in the backup dir
        blobs = azfunc.get_blob_list(self.storage_account, self.container_name, self.blob_path)

        if len(blobs) == 0:
            print(f"No backups found in {self.blob_path}")
            return None

        # get the latest backup
        latest_backup = sorted(blobs, key=lambda x: x.name)[-1]

        #return latest_backup #this is the path relative to the container

        source = Path(self.storage_account, self.container_name, latest_backup.name).as_posix()
        filename = Path(latest_backup.name).name
        destination = Path("/tmp", filename).as_posix()

        print(f"Downloading {source} to {destination}")
        azfunc.download_blob(source, destination)

    def restore_latest_backup(self):
        """
        Restore the latest backup from the blob path
        """



def main():

    pgbackup = AzPGBackup(storage_account="storbackupspocprod1",
                        container_name="pgbackups",
                        blob_path="db",
                        backup_prefix="fieldwork",
                        db_name="fieldwork",
                        db_user="fieldwork",
                        db_password="fieldwork",
                        db_host="localhost")

    pgbackup.get_latest_backup()




if __name__ == "__main__":
    main()

