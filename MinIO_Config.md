# Install the MinIO Server

Download the MinIO executable from the following URL: [text](https://dl.min.io/server/minio/release/windows-amd64/minio.exe)

# Launch the minio server

In PowerShell or the Command Prompt, navigate to the location of the executable or 
add the path of the minio.exe file to the system $PATH.
Run **.\minio.exe server C:\minio --console-address :9001** 
replace C:\minio with location of the executable

# Connect your Browser to the MinIO Server

1. Access the MinIO Console by going to a browser and going to http://127.0.0.1:9001

2. Log in to the Console with the RootUser and RootPass user credentials displayed in the output. These default to minioadmin | minioadmin.

# Use MinIO
## Create buckets
1. Go to buckets in left pannel
2. Create 3 buckets *raw, processed, analytics*

## Upload files to bucket
Run upload.py to upload json files

## Retrieve files from bucket
Run retrieve.py to download files from buckets

***For more information, read https://min.io/docs/minio/windows/index.html***