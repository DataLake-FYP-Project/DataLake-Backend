# Install the MinIO Server

Download the MinIO executable from the following URL: [text](https://dl.min.io/server/minio/release/windows-amd64/minio.exe)

# Launch the minio server

In PowerShell or the Command Prompt, navigate to the location of the executable or 
add the path of the minio.exe file to the system $PATH.
Run **.\minio.exe server C:\minio --console-address :9001** 
replace C:\minio with location of the executable

# Connect your Browser to the MinIO Server

Access the MinIO Console by going to a browser and going to http://127.0.0.1:9001

***For more information, read https://min.io/docs/minio/windows/index.html***