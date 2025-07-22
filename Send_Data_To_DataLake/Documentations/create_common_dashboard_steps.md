## Steps to Create Visualizations in Kibana
## Prerequisites
1. Verify index is searchable
> Go to: Kibana → Stack Management → Index Patterns

2. Create/select: datalake-common-data

> Set @timestamp as the default time field

3. Go to Dashboard

> Navigate to Dashboard on the sidebar

4. Click Create Dashboard

> Add the visualizations below

## Visualizations
1. Total Detections
- **Type**: `Metric`
- Metric: Count 
- Index: datalake-common-data 
- Title: Total Detections

2. Class-wise Detection Distribution
- **Type**: `Pie`
- Slice By: class_name.keyword 
- Metric: Count 
- Title: Detected Classes Distribution

3. Detection Confidence Range
- **Type**: `Vertical Bar`
- X-Axis: class_name.keyword 
- Y-Axis: Average of confidence 
- Title: Average Detection Confidence by Class

4. Detections Over Time
- **Type**: `Line Chart`
- X-Axis: @timestamp (Date Histogram)
- Y-Axis: Count 
- Title: Detections Over Time

5. Bounding Box Width vs Height (Scatter Plot)
- **Type**: `Lens or Vega (Advanced)`
- X-Axis: scripted_field: bbox.x2 - bbox.x1 as width 
- Y-Axis: scripted_field: bbox.y2 - bbox.y1 as height 
- Title: Object Size Distribution

You need to define scripted fields bbox_width and bbox_height in index pattern:

6. Top Tracker IDs (most active)
- **Type**: `Horizontal Bar`
- X-Axis: tracker_id 
- Y-Axis: Count 
- Title: Top Tracker IDs by Detections

7. Class-wise Confidence Table
- **Type**: `Data Table`
- Columns:class_name.keyword 
- Average of confidence 
- Max confidence 
- Min confidence 
- Title: Detection Confidence Stats by Class

8. Frame-wise Detection Count
- **Type**: `Line Chart`
- X-Axis: frame_number 
- Y-Axis: Count 
- Title: Detections per Frame