# azcmd

A simplified azure command line tool for humans. Uses Managed Identity as default for login. (You must set the default
tenant if you have multiple tenants in your account.)

## Storage and Containers

### Listing Storage Accounts

- az-blob ls  - lists all storage accounts 
- az-blob ls  <storage_account> - lists all containers in a storage account and all blobs in a container
- az-blob ls <storage_account>/<container> - lists all blobs in a container

### Downloading and Uploading Blobs

#### Get
- az-blob get storage_account/container/blob - downloads a blob to the current directory. If the blob has subdirectories, they will be created.
- az-blob get --latest storage_account/container/blob - downloads the latest version of a blob to the current directory. 

#### Put
- az-blob put filename storage_account/container/blob - uploads a file to a blob. If the blob has subdirectories, they will be created.

