# Create Index in Kibana (Elasticsearch) using Dev Tools & Create Dashboard

## Steps to Follow

### 1. Create Index (Go to Kibana → Dev Tools)

Run the below command to create an index:

```json
PUT /datalake-vehicle-data
{
"mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "source_file": { "type": "keyword" },
      "processing_date": { "type": "date" },
      "processing_version": { "type": "keyword" },
      "vehicle_count": { "type": "integer" },
      "vehicle_id": { "type": "integer" },
      "vehicle_type": { "type": "keyword" },
      "vehicle_color": { "type": "keyword" },
      "confidence_avg": { "type": "float" },
      "first_detection": { "type": "date" },
      "last_detection": { "type": "date" },
      "duration_seconds": { "type": "float" },
      "stopped": { "type": "boolean" },
      "frame_count": { "type": "integer" },
      "avg_speed": { "type": "float" },
      "max_speed": { "type": "float" },
      "min_speed": { "type": "float" },
      "speed_variation": { "type": "float" },
      "lane_changes": { "type": "integer" },
      "initial_lane": { "type": "keyword" },
      "final_lane": { "type": "keyword" },
      "most_common_lane": { "type": "keyword" },
      "lane_change_frequency": { "type": "float" },
      "line_crossing_violation": { "type": "boolean" },
      "direction": { "type": "keyword" },
      "direction_changes": { "type": "integer" },
      "time_spent_per_direction_Unknown": { "type": "float" },
      "time_spent_per_direction_Down": { "type": "float" },
      "time_spent_per_direction_Up": { "type": "float" },
      "red_light_violation": { "type": "boolean" },
      "total_distance": { "type": "float" },
      "movement_angles": { "type": "float" },
      "avg_movement_angle": { "type": "float" }
    }
  }
}

```
### 2. Create a Data View (Go to Stack management → Kibana → Data Views)
datalake-vehicle-data

### 3. Create a timestamp pipeline (Go to Kibana → Dev Tools)
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



### -----------For Testing Purpose----------------
### 4. Enter data to the index
```json
POST /datalake-vehicle-data/_doc?pipeline=vehicle_data_timestamp_pipeline
{
  "@timestamp": "2025-05-14T12:00:00Z",
  "source_file": "camera_feed_001.mp4",
  "processing_date": "2025-05-14",
  "processing_version": "v1.0",
  "vehicle_count": 1,
  "vehicle_id": 12345,
  "vehicle_type": "Car",
  "vehicle_color": "Red",
  "confidence_avg": 0.98,
  "first_detection": "2025-05-14T11:59:50Z",
  "last_detection": "2025-05-14T12:00:10Z",
  "duration_seconds": 20.0,
  "stopped": false,
  "frame_count": 300,
  "avg_speed": 45.5,
  "max_speed": 60.0,
  "min_speed": 30.0,
  "speed_variation": 5.2,
  "lane_changes": 2,
  "initial_lane": "Lane 1",
  "final_lane": "Lane 2",
  "most_common_lane": "Lane 1",
  "lane_change_frequency": 0.1,
  "line_crossing_violation": false,
  "direction": "Up",
  "direction_changes": 1,
  "time_spent_per_direction_Unknown": 0.0,
  "time_spent_per_direction_Down": 0.0,
  "time_spent_per_direction_Up": 20.0,
  "red_light_violation": false,
  "total_distance": 500.0,
  "movement_angles": [
    57.24872961031495, 43.4392972644037, 45.66630597876039, 40.15921856233296,
    53.184810576944756, 49.403049168867625, 48.72307764905442, 44.52687729279536,
    35.40816645035203, 48.29332308869407, 48.06274678775557, 34.756282618036074,
    43.437476077688814, 43.22561764586927, 26.275023558358203, 13.187796670531867,
    14.39504991869238, -7.138922517082822, 27.034685426713025, 14.97942454987065,
    9.995548185155874, 18.882336383088322, 10.869179064255135, 11.22310921016996
  ],
  "avg_movement_angle": 32.30159205090093
}
```
### 5. Verify Data
```json
GET datalake-vehicle-data/_search
```

### 6. Delete index
```json
DELETE datalake-vehicle-data
```

### 7. Get all indices
```json
GET _cat/indices?v
```
