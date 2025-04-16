# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow
### 1. Create a Data View
datalake-vehicle-data

### 1. Create a timestamp pipeline
```json
PUT _ingest/pipeline/vehicle_data_timestamp_pipeline
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
PUT datalake-vehicle-data
{
  "mappings": {
    "properties": {
      "tracker_id": { "type": "integer" },
      "class_id": { "type": "integer" },
      "average_confidence": { "type": "float" },
      "class_name": { "type": "keyword" },
      "direction": { "type": "keyword" },
      "lane": { "type": "keyword" },
      "vehicle_color": { "type": "keyword" },
      "stopped": { "type": "boolean" },
      "speed": { "type": "float" },
      "confidence": { "type": "float" },
      "bbox": { "type": "float" },
      "entry_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
      "exit_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
      "red_line_violation": { "type": "boolean" },
      "red_line_violation_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
      "double_line_crossing": { "type": "boolean" },
      "double_line_crossing_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
      "@timestamp": { "type": "date" }
    }
  }
}

```


### 3. Enter data to the index
```json
POST datalake-vehicle-data/_doc?pipeline=vehicle_data_timestamp_pipeline
{
  "tracker_id": 3,
  "class_id": 1,
  "average_confidence": 0.85,
  "class_name": "car",
  "direction": "Up",
  "lane": "Left Lane",
  "vehicle_color": "red",
  "stopped": false,
  "speed": 55.5,
  "confidence": 0.95,
  "bbox": [34.5, 23.7, 45.8, 30.6],
  "entry_time": "2025-04-10 14:23:45",
  "exit_time": "2025-04-10 14:25:00",
  "red_line_violation": false,
  "red_line_violation_time": "2025-04-10 14:24:00",
  "double_line_crossing": false,
  "double_line_crossing_time": "2025-04-10 14:24:30"
}

```
### 4. Verify Data
```json
GET datalake-vehicle-data/_search
```

### 5. Delete index
```json
DELETE datalake-vehicle-data
```
