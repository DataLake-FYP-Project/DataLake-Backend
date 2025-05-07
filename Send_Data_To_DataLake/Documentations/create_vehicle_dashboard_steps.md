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

![Image](https://github.com/user-attachments/assets/762adc14-5d97-4dae-a712-a90912b2c86d)


2. Vehicle Type Distribution

* Visualization Type  -  Pie
* Slide by - field - vehicle_type
* Metric - Count of Records
* Index Pattern(Data view) - datalake-vehicle-data
* title - Vehicle Type Distribution

![Image](https://github.com/user-attachments/assets/c05ba294-9596-4b9e-834f-ab3abc1edaf7)
