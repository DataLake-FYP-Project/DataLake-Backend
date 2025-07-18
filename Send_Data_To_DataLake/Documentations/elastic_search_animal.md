# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow

### 1. Create Index (Go to Kibana → Dev Tools)

Run the below command to create an index:

```json
PUT /datalake-animal-data
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "animal_type": { "type": "keyword" },
      "confidence": { "type": "float" },
      "area": { "type": "integer" },
      "frame_number": { "type": "integer" },
      "video_timestamp": { "type": "float" }
    }
  }
}

```

### 2. Create a Data View (Go to Stack management → Kibana → Data Views)
datalake-animal-data

### 3. Create a timestamp pipeline (Go to Kibana → Dev Tools)
```json
PUT _ingest/pipeline/animal_data_timestamp_pipeline
{
  "description": "Add timestamp to animal detection data",
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
POST /datalake-animal-data/_doc?pipeline=animal_data_timestamp_pipeline
{
  "animal_type": "cow",
  "confidence": 0.7433731555938721,
  "area": 267528,
  "frame_number": 5,
  "video_timestamp": 0.07120063821300358
}

```

### 5. Verify Data
```json
GET datalake-animal-data/_search
```

### 6. Delete index
```json
DELETE datalake-animal-data
```

### 7. Get all indices
```json
GET _cat/indices?v
```

