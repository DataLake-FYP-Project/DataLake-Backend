1. First Create Indices

```json
PUT /datalake-school-data
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "event_id": { "type": "integer" },
      
      "alert_level_distribution": {
        "properties": {
          "high": { "type": "integer" },
          "low": { "type": "integer" }
        }
      },
      
      "avg_confidence": { "type": "float" },
      "avg_event_duration": { "type": "float" },
      "duration_seconds": { "type": "float" },
      
      "start_time": { "type": "date" },
      "end_time": { "type": "date" },
      
      "event_duration_stddev": { "type": "float" },
      "event_type": { "type": "keyword" },
      "frame_occurrences": { "type": "integer" },
      
      "involved_persons": { "type": "keyword" },
      "unique_roles": { "type": "keyword" },
      
      "location": { "type": "keyword" },
      "most_common_alert_level": { "type": "keyword" },
      "most_common_role": { "type": "keyword" },
      
      "multiple_persons_involved": { "type": "boolean" },
      "notes": { "type": "text" },
      "response_required": { "type": "boolean" }
    }
  }
}
```


```json
PUT /datalake-retail-data
{
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "detection_id": { "type": "integer" },
      "average_price": { "type": "float" },
      "average_stock_level": { "type": "float" },
      "category": { "type": "keyword" },
      "frame_appearances": { "type": "integer" },
      "location": { "type": "keyword" },
      "picked_by_customer": { "type": "boolean" },
      "product_name": { "type": "keyword" },
      "unique_expiry_dates": { "type": "date" },
      "source_file": { "type": "keyword" },
      "processing_date": { "type": "date" },
      "processing_version": { "type": "keyword" }
    }
  }
}
```
2. Second Create Data views for these two indices. 

3. Third Create common pipeline. 

```json
PUT _ingest/pipeline/timestamp_pipeline
{
  "description": "Add timestamp",
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

4. Run below command

```
python -m streamlit run app.py

```
