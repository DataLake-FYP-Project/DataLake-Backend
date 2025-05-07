## Steps to Create Visualizations in Kibana
#### Prerequisite
1. Make sure your index is searchable in Kibana:
```
Go to Kibana → Stack Management → Index Patterns
```

2. Create/select index pattern: datalake-vehicle-data

3. Set @timestamp as the default time field

4. Go to Dashboard
* Click Dashboard in the left-hand menu
* Click Create Dashboard
* Create Visualizations

### Visualizations
1. Vehicle Count Over Time

* Visualization Type  -  Legacy Metric
* Metric - Count of Records
* Index Pattern(Data view) - datalake-vehicle-data
* title - Total vehicle count over time


2. Vehicle Type Distribution

* Visualization Type  -  Pie
* Slide by - field - vehicle_type
* Metric - Count of Records
* Index Pattern(Data view) - datalake-vehicle-data
* title - Vehicle Type Distribution