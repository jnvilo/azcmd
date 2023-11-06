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
        self.container_name = path_parts[1]
        self.blob_name = "/".join(path_parts[2:])
        return self

    @property
    def url(self):
        return f"https://{self.storage_account}.blob.core.windows.net/{self.container_name}/{self.blob_name}"

