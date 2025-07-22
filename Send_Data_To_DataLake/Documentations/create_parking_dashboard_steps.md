## Steps to Create Visualizations in Kibana
#### Prerequisite
1. Make sure your index is searchable in Kibana:
   > Go to Kibana → Stack Management → Index Patterns


3. Create/select index pattern:
   > datalake-parking-data

4. Set @timestamp as the default time field

5. Go to Dashboard
    > Click Dashboard in the left-hand menu

   > Click Create Dashboard

    > Create Visualizations

### Visualizations
---

### 1. Total Parking Slots

- **Type**: `Metric`
- **Metric**: `Count`
- **Index**: `datalake-parking-data`
- **Title**: `Total Parking Slots`
- **Field**: `slot_id`

---

### 2. Final Occupancy Status

- **Type**: `Pie`
- **Slice By**: `slot_status.keyword`
- **Metric**: `Count`
- **Title**: `Current Slot Occupancy`

---

### 3. Free Time % per Slot

- **Type**: `Vertical Bar`
- **X-Axis**: `slot_id`
- **Y-Axis**: `free_percentage`
- **Title**: `Free Time Percentage`

---

### 4. Number of Parking Sessions

- **Type**: `Vertical Bar`
- **X-Axis**: `slot_id`
- **Y-Axis**: Count of `parking_sessions`
- **Title**: `Parking Sessions per Slot`

---

### 5. Total Occupied Time

- **Type**: `Line Chart`
- **X-Axis**: `slot_id`
- **Y-Axis**: `total_occupied_seconds`
- **Title**: `Total Occupied Time by Slot`

---

### 6. Final Occupancy Table

- **Type**: `Data Table`
- **Columns**: `slot_id`, `slot_status`
- **Title**: `Final Occupancy Status Table`

---

### 7. Slot State Transitions

- **Type**: `Clustered Bar`
- **X-Axis**: `slot_id`
- **Y-Axis**: 
  - Metric 1: `state_transitions.became_free`
  - Metric 2: `state_transitions.became_occupied`
- **Title**: `Slot Transition Counts`

---