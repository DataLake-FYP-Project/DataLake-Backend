# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow

### 1. Create Index (Go to Kibana → Dev Tools)

Run the below command to create an index:

```json
PUT /datalake-parking-data
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "slot_id": { "type": "integer" },
      "slot_status": { "type": "keyword" },
      "total_occupied_seconds": { "type": "float" },
      "total_free_seconds": { "type": "float" },
      "free_percentage": { "type": "float" },
      "became_free": { "type": "integer" },
      "became_occupied": { "type": "integer" },
      "entry_time": { "type": "float" },
      "exit_time": { "type": "float" },
      "duration": { "type": "float" }
    }
  }
}

```

### 2. Create a Data View (Go to Stack management → Kibana → Data Views)
datalake-parking-data

### 3. Create a timestamp pipeline (Go to Kibana → Dev Tools)
```json
PUT _ingest/pipeline/parking_data_timestamp_pipeline
{
  "description": "Add timestamp to parking detection data",
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

### -----------For Testing Purpose----------------
### 4. Enter data to the index
```json
POST /datalake-parking-data/_doc?pipeline=parking_data_timestamp_pipeline
{
  "slot_id": 1,
  "slot_status": "occupied",
  "total_occupied_seconds": 235.5,
  "total_free_seconds": 64.2,
  "free_percentage": 0.214,
  "became_free": 2,
  "became_occupied": 3,
  "entry_time": 0.5,
  "exit_time": 65.0,
  "duration": 64.5
}


```

### 5. Verify Data
```json
GET datalake-parking-data/_search
```

### 6. Delete index
```json
DELETE datalake-parking-data
```

### 7. Get all indices
```json
GET _cat/indices?v
```

