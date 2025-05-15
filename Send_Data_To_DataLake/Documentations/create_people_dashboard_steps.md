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