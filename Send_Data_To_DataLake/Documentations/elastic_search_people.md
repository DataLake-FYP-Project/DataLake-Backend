# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow

### 1. Create Index (Go to Kibana → Dev Tools)

Run the below command to create an index:

```json
PUT /datalake-people-data
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "people_id": { "type": "integer" },
      "age": { "type": "keyword" },
      "gender": { "type": "keyword" },
      "carrying": { "type": "keyword" },
      "confidence_avg": { "type": "float" },
      "entered_restricted_area": { "type": "boolean" },
      "restricted_area_entry_time": { "type": "date", "null_value": null },
      "entry_time": { "type": "date" },
      "exit_time": { "type": "date" },
      "duration_seconds": { "type": "double" },
      "frame_count": { "type": "integer" }
    }
  }
}

```

### 2. Create a Data View (Go to Stack management → Kibana → Data Views)
datalake-people-data

### 3. Create a timestamp pipeline (Go to Kibana → Dev Tools)
```json
PUT _ingest/pipeline/people_data_timestamp_pipeline
{
  "description": "Add timestamp to people data",
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
POST /datalake-people-data/_doc?pipeline=people_data_timestamp_pipeline
{
  "@timestamp": "2025-05-06T06:41:01", 
  "people_id": 1,
  "age": "30",
  "gender": "Man",
  "carrying": "none",
  "confidence_avg": 0.875567140430212,
  "entered_restricted_area": false,
  "restricted_area_entry_time": null,
  "entry_time": "2025-05-06T06:41:01",
  "exit_time": "2101-01-29T17:53:46",
  "duration_seconds": 2389950765.0,
  "frame_count": 40
}
```

### 5. Verify Data
```json
GET datalake-people-data/_search
```

### 6. Delete index
```json
DELETE datalake-people-data
```

### 7. Get all indices
```json
GET _cat/indices?v
```

