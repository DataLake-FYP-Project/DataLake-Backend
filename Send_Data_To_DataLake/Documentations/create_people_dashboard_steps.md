## Steps to Create Visualizations in Kibana
#### Prerequisite
1. Make sure your index is searchable in Kibana:
   > Go to Kibana → Stack Management → Index Patterns


3. Create/select index pattern:
   > datalake-people-data

4. Set @timestamp as the default time field

5. Go to Dashboard
    > Click Dashboard in the left-hand menu

   > Click Create Dashboard

    > Create Visualizations

### Visualizations
1. People Count Over Time

- **Visualization Type**: `Legacy Metric`  
- **Metric**: `Count of Records`  
- **Index Pattern (Data view)**: `datalake-people-data`  
- **Title**: `Total people count over time`
  
<img src="https://github.com/user-attachments/assets/6733616b-5415-45b1-97c3-27aa0b0e6ce8" width="500" />

---

2. People Age Distribution

- **Visualization Type**: `Bar Vertical`  
- **Horizontal axis**: `Top 5 values of age`
- **Vertical axis**: `Count of records`  
- **Index Pattern (Data view)**: `datalake-people-data`  
- **Title**: `Age Group Distribution.`
  
<img src="https://github.com/user-attachments/assets/9f7faf4a-14e1-4c22-996e-78326613a606" width="500" />
<img src="https://github.com/user-attachments/assets/60efd8f4-4ea0-4df9-89e8-38546529b3a6" width="500" />

---

3. People Gender distribution

- **Visualization Type**: `Pie chart`  
- **Metric**: `Count of Records`
- **slice By**:`gender`
- **Index Pattern (Data view)**: `datalake-people-data`  
- **Title**: `Gender Group Distribution.`
  
<img src="https://github.com/user-attachments/assets/cf7e99dd-0256-480f-8604-fe27309e27ed" width="500" />
<img src="https://github.com/user-attachments/assets/addb2911-03f1-4aff-a5cc-e48cd3c1f625" width="500" />
---
