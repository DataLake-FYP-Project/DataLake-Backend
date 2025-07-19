
### Steps to Create Visualizations for Safety Data in Kibana
## Prerequisite
1️⃣ Make sure your safety index is searchable:

Kibana → Stack Management → Index Patterns

2️⃣ Create/select index pattern:

datalake-safety-data

3️⃣ Set @timestamp as the default time field

4️⃣ Go to Dashboard:

Click Dashboard → Create Dashboard → Add Visualizations

### Visualizations
1️⃣ Total Detections Over Time
Visualization Type: Metric

Metric: Count of Records

Index Pattern: datalake-safety-data

- **Title**: Total People Detections Over Time

2️⃣ Hardhat Compliance

- **Visualization Type**: `Pie`
- **Slice by**: field - `wearing_hardhat`
- **Metric**:`Count of Records`
- **Index Pattern**: `datalake-safety-data`

- **Title**: Hardhat Compliance

✅ Shows how many people wore hardhats vs. did not.

3️⃣ Safety Vest Compliance

- **Slice by**: field - `wearing_safety_vest`
- **Metric**: `Count of Records`
- **Index Pattern**: `datalake-safety-data`

- **Title**: Safety Vest Compliance

4️⃣ Mask Compliance

- **Visualization Type**: `Pie`
- **Slice by**: field - `wearing_mask`
- **Metric**: `Count of Records`
- **Index Pattern**: `datalake-safety-data`

- **Title**: Mask Compliance

5️⃣ Safety Status Summary

- **Visualization Type**: `Pie`
- **Slice by**: field - `safety_status`
- **Metric**: `Count of Records`
- **Index Pattern**: `datalake-safety-data`

- **Title**: Overall Safety Status

6️⃣ Top Missing Items

- **Visualization Type**: `Bar`
- **Horizontal Axis**: field - `missing_items`
- **Vertical Axis**: `Count of Records`
- **Index Pattern**: `datalake-safety-data`

- **Title**: Most Common Missing Safety Items

✅ If missing_items is an array field, ensure your mapping supports keyword.

7️⃣ Detections Per Frame

- **Visualization Type**: Line
- **Horizontal Axis**: field - frame_number
- **Vertical Axis**: Count of Records
- **Index Pattern**: datalake-safety-data

- **Title**: Detections Per Frame

✅ Helps you see how many people were detected in each frame.
