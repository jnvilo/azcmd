from dataclasses import dataclass
@dataclass
class BlobInfo:

    storage_account: str = None
    container_name: str = None
    blob_name: str = None

    def from_path(self, path):
        """
        Create a BlobPath from a string path
        """
        if path.startswith("/"):
            path = path[1:]

        path_parts = path.split("/")
        self.storage_account = path_parts[0]
        if len(path_parts) >= 2:
            self.container_name = path_parts[1]
            if len(path_parts) >= 3: 
                self.blob_name = "/".join(path_parts[2:])
        return self

    @property
    def has_blob(self):
        return self.blob_name is not None

    @property
    def has_container(self):
        return self.container_name is not None

    @property
    def has_storage_account(self):
        return self.storage_account is not None

    @property
    def url(self):
        return f"https://{self.storage_account}.blob.core.windows.net/{self.container_name}/{self.blob_name}"

    @property
    def container_url(self):
        return f"https://{self.storage_account}.blob.core.windows.net/{self.container_name}"

    @property
    def account_url(self):
        return f"https://{self.storage_account}.blob.core.windows.net"

    def as_dict(self):
        return {
            "storage_account": self.storage_account,
            "container_name": self.container_name,
            "blob_name": self.blob_name,
            "url": self.url,
            "container_url": self.container_url,
            "account_url": self.account_url
        }





