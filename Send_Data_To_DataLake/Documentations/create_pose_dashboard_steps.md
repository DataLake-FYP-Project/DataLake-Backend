### Steps to Create Visualizations for Pose Data in Kibana
## Prerequisite
1️⃣ Make sure your pose index is searchable:

Kibana → Stack Management → Index Patterns

2️⃣ Create/select index pattern:

datalake-pose-data

3️⃣ Set @timestamp as the default time field

4️⃣ Go to Dashboard:

Click Dashboard → Create Dashboard → Add Visualizations

### Visualizations

- **Total Detections Over Time**

- Visualization Type: Metric
- Metric: Count of Records
- Index Pattern: datalake-pose-data

Kibana Dashboard Setup: Pose Data Insights

- **Total Pose Detections Over Time**

- Type: Metric 
- Metric: Count of Records
- Index Pattern: datalake-pose-data

- **Title**: `Total Pose Detection`

Action Type Distribution

- Type: `Pie`
- Slice by: `action`
- Metric: `Count`

- **Title**: `Action Distribution` (Standing, Sitting, etc.)

Detections Per Frame

- Type: `Line`
- X-Axis: `frame_number`
- Y-Axis: `Count of Records`

- **Title**: Detections Per Frame

Confidence Level Histogram

- Type: `Histogram`
- X-Axis: `confidence`
- Y-Axis: `Count of Records`

- **Title**: `Detection Confidence Distribution`

Top Actions by Frequency

- Type: `Bar`
- X-Axis: `action`
- Y-Axis: `Count of Records`

- **Title**: `Most Frequent Actions`

Average Confidence per Action

- Type: `Bar`
- X-Axis: `action`
- Y-Axis: `Average of confidence`

- **Title**: `Average Confidence per Action`
