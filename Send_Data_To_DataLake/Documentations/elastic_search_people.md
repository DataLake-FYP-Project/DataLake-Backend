# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow
### 1. Create a Data View
datalake-people-data

### 1. Create a timestamp pipeline
```json
PUT _ingest/pipeline/people_data_timestamp_pipeline
{
  "description": "Add timestamp to vehicle data",
  "processors": [
    {
      "set": {
        "field": "@timestamp",
        "value": "{{_ingest.timestamp}}"
      }
    }
  ]
}
```

### 1. Go to Kibana → Dev Tools

### 2. Create Index

Run the below command to create an index:

```json
PUT datalake-people-data
{
  "mappings": {
    "properties": {
      "video_metadata": {
        "properties": {
          "filename": { "type": "text" },
          "format_name": { "type": "text" },
          "format_long_name": { "type": "text" },
          "duration_seconds": { "type": "float" },
          "size_bytes": { "type": "long" },
          "bitrate": { "type": "long" },
          "creation_time": { "type": "date", "format": "strict_date_time" },
          "encoder": { "type": "text" },
          "video_codec": { "type": "text" },
          "width": { "type": "integer" },
          "height": { "type": "integer" },
          "fps": { "type": "float" },
          "device_model": { "type": "text" },
          "software": { "type": "text" },
          "audio_codec": { "type": "text" },
          "sample_rate": { "type": "integer" },
          "channels": { "type": "integer" },
          "creation_time_utc": { "type": "date", "format": "strict_date_time" },
          "creation_time_local": { "type": "text" },
          "recording_time": { "type": "text" }
        }
      },
      "processing_time": { "type": "date", "format": "strict_date_time" },
      "summary": {
        "properties": {
          "total_people": { "type": "integer" },
          "total_entering": { "type": "integer" },
          "total_exiting": { "type": "integer" },
          "restricted_area_entries": { "type": "integer" },
          "restricted_people_ids": { "type": "keyword" },
          "fps": { "type": "float" },
          "duration_seconds": { "type": "float" }
        }
      },
      "detections": {
        "type": "nested",
        "properties": {
          "tracker_id": { "type": "integer" },
          "gender": { "type": "text" },
          "age": { "type": "keyword" },
          "carrying": { "type": "text" },
          "confidence": { "type": "float" },
          "entry_time": { "type": "date", "format": "strict_date_time" },
          "exit_time": { "type": "date", "format": "strict_date_time" },
          "entry_frame": { "type": "integer" },
          "exit_frame": { "type": "integer" },
          "entered_restricted": { "type": "boolean" },
          "restricted_entry_time": { "type": "date", "format": "strict_date_time" },
          "restricted_exit_time": { "type": "date", "format": "strict_date_time" }
        }
      }
    }
  }
}

```


### 3. Enter data to the index
```json
POST datalake-people-data/_doc?pipeline=people_data_timestamp_pipeline
{
  "video_metadata": {
    "filename": "uploads\\people_video.mp4",
    "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
    "format_long_name": "QuickTime / MOV",
    "duration_seconds": 19.754667,
    "size_bytes": 5580695,
    "bitrate": 2260000,
    "creation_time": "2025-04-08T11:45:03.000000Z",
    "encoder": null,
    "video_codec": "h264",
    "width": 960,
    "height": 544,
    "fps": 29.97011426106062,
    "device_model": null,
    "software": null,
    "audio_codec": "aac",
    "sample_rate": 48000,
    "channels": 2,
    "creation_time_utc": "2025-04-08T11:45:03Z",
    "creation_time_local": "2025-04-08 17:15:03 Sri Lanka Standard Time",
    "recording_time": "2025-04-08 17:15:03 Sri Lanka Standard Time"
  },
  "processing_time": "2025-04-14T03:54:52Z",
  "summary": {
    "total_people": 5,
    "total_entering": 1,
    "total_exiting": 4,
    "restricted_area_entries": 0,
    "restricted_people_ids": [],
    "fps": 29.0,
    "duration_seconds": 20.413793103448278
  },
  "detections": [
    {
      "tracker_id": 2,
      "gender": "Man",
      "age": "25",
      "carrying": "no objects",
      "confidence": 0.9028873443603516,
      "entry_time": null,
      "exit_time": "2025-04-08T11:45:05Z",
      "entry_frame": null,
      "exit_frame": 66,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    },
    {
      "tracker_id": 5,
      "gender": "Unknown",
      "age": "Unknown",
      "carrying": "none",
      "confidence": 0.7893534302711487,
      "entry_time": null,
      "exit_time": null,
      "entry_frame": null,
      "exit_frame": null,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    },
    {
      "tracker_id": 4,
      "gender": "Man",
      "age": "39",
      "carrying": "no objects",
      "confidence": 0.9234234690666199,
      "entry_time": null,
      "exit_time": "2025-04-08T11:45:06Z",
      "entry_frame": null,
      "exit_frame": 104,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    },
    {
      "tracker_id": 10,
      "gender": "Man",
      "age": "31",
      "carrying": "no objects",
      "confidence": 0.8687556982040405,
      "entry_time": null,
      "exit_time": "2025-04-08T11:45:15Z",
      "entry_frame": null,
      "exit_frame": 376,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    },
    {
      "tracker_id": 6,
      "gender": "Man",
      "age": "31",
      "carrying": "no objects",
      "confidence": 0.8894902467727661,
      "entry_time": null,
      "exit_time": "2025-04-08T11:45:17Z",
      "entry_frame": null,
      "exit_frame": 426,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    },
    {
      "tracker_id": 15,
      "gender": "Man",
      "age": "36",
      "carrying": "no objects",
      "confidence": 0.9223424792289734,
      "entry_time": "2025-04-08T11:45:21Z",
      "exit_time": null,
      "entry_frame": 537,
      "exit_frame": null,
      "entered_restricted": false,
      "restricted_entry_time": null,
      "restricted_exit_time": null
    }
  ]
}



```
### 4. Verify Data
```json
GET datalake-people-data/_search
```

### 5. Delete index
```json
DELETE datalake-people-data
```
