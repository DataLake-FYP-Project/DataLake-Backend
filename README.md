# Elasticsearch and Kibana Setup Using Docker

## Prerequisites

- Install **Docker** from [Docker Official Site](https://www.docker.com/get-started).
- Ensure **Docker Compose** is installed.
- Install MinIo, Json, Elasticsearch and kibana

## Step 1: Run the `docker-compose.yml` File
  
## Step 2: Start the containers by running following command(DataLake-Backend directory)
    docker-compose up -d

This will start:

Elasticsearch on http://localhost:9200

Kibana on http://localhost:5601

# MinIo Setup

Go to the Minio EXE folder and run following in cmd.
```bash
minio server C:\minio --console-address ":9001"

```

And then access the MinIo   http://127.0.0.1:9001



# Vehicle Data Upload to MinIO and Elasticsearch

## Prerequisites

Before starting, ensure you have the following prerequisites set up:

1. **MinIO Bucket:**
   - Create a MinIO bucket named `vehicle-data` to store the uploaded files.
   - Ensure MinIO is running and accessible.

2. **ElasticSearch (ELK Stack):**
   - Make sure Elasticsearch is installed and configured for data storage.

---

## Backend Setup

The project contains two backends: one for generating JSON data from the model and the other for sending the data to MinIO and Elasticsearch.

### 1. **Start backends**

Go to the `run_all_services.ps1` file and run the following command to generate JSON data from the model:

```bash
powershell -ExecutionPolicy Bypass -File .\run_all_services.ps1
```

## Uploading Files via Streamlit UI

1. Once the UI is open in your browser, you will be able to upload video files.
2. Choose the video file to upload and click the upload button.
3. If the file is successfully uploaded to MinIO and Elasticsearch, you will see the following message on the UI:

   **`Video uploaded successfully!`**

