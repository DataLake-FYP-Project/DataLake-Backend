## Steps to Create Visualizations in Kibana
#### Prerequisite
1. Make sure your index is searchable in Kibana:
   > Go to Kibana → Stack Management → Index Patterns


3. Create/select index pattern:
   > datalake-vehicle-data

4. Set @timestamp as the default time field

5. Go to Dashboard
    > Click Dashboard in the left-hand menu

   > Click Create Dashboard

    > Create Visualizations

### Visualizations
1. Vehicle Count Over Time

- **Visualization Type**: `Legacy Metric`  
- **Metric**: `Count of Records`  
- **Index Pattern (Data view)**: `datalake-vehicle-data`  
- **Title**: `Total vehicle count over time`
  
<img src="https://github.com/user-attachments/assets/762adc14-5d97-4dae-a712-a90912b2c86d" width="500" />

---

2. Vehicle Type Distribution

- **Visualization Type**:  `Pie`
- **Slide by**: `field - vehicle_type`
- **Metric**: Count of Records
- **Index Pattern(Data view)**: `datalake-vehicle-data`
- **Title**: `Vehicle Type Distribution`
  
<img src="https://github.com/user-attachments/assets/c05ba294-9596-4b9e-834f-ab3abc1edaf7" width="500" />

---

3. Avearge Speed Over Each Vehicle ID

- **Visualization Type**  :  `Line`
- **Horizontal Axis** : `field - vehicle_id`
- **Vertical Axis** : `Maximum of Avg_Speed`
- **Index Pattern(Data view)** : `datalake-vehicle-data`
- **Title** : `Avearge Speed Over Each Vehicle ID`
  
<img src="https://github.com/user-attachments/assets/b2919404-d19b-4d90-a6ff-90ed5782005b" width="500" />
<img src="https://github.com/user-attachments/assets/4cb9e1f4-3c54-4527-bc93-48baf7217efc" width="500" />

---

4. Maximum Speed Over Each Vehicle ID

- **Visualization Type** :  `Bar`
- **Horizontal Axis** : `field vehicle_id`
- **Vertical Axis** : `Maximum of Max_Speed`
- **Index Pattern(Data view)** : `datalake-vehicle-data`
- **Title** : `Maximum Speed Over Each Vehicle ID`
  
<img src="https://github.com/user-attachments/assets/40ac0f0a-f3de-4b0d-a10e-6468a8cd9fb8" width="500" />
<img src="https://github.com/user-attachments/assets/4e0d2fa9-067b-4e33-a5fa-7da3d7c9f456" width="500" />

---

5. Distribution of Direction

- **Visualization Type** :  `Pie`
- **Slide by** : `field - direction`
- **Metric** : `Count of Records`
- **Index Pattern(Data view)** : `datalake-vehicle-data`
- **Title** : `Distribution of Direction`
  
<img src="https://github.com/user-attachments/assets/2f62f9c2-f01f-4aa6-8195-9aa9516a6de9" width="500" />
<img src="https://github.com/user-attachments/assets/544c19aa-997d-46b4-8bb1-7edeed5b691f" width="500" />

---

6. Most Common Lane

- **Visualization Type** :  `Bar`
- **Horizontal Axis** : `field - most_common_lane`
- **Vertical Axis** : `Count of Records`
- **Index Pattern(Data view)** : `datalake-vehicle-data`
- **Title** : `Most Common Lane`
  
<img src="https://github.com/user-attachments/assets/237c5cf2-afb1-4fd2-bcab-6ed85a69299f" width="500" />
<img src="https://github.com/user-attachments/assets/aad94bd5-4c5b-40d4-93ab-583c33914553" width="500" />

---

7. Duration of each vehicle

- **Visualization Type** :  `Line`
- **Horizontal Axis** : `field - Vehicle_id`
- **Vertical Axis** : `Max of duration seconds`
- **Index Pattern(Data view)** : `datalake-vehicle-data`
- **Title** : `Duration of each vehicle`
  
<img src="https://github.com/user-attachments/assets/af0d2b80-5407-4c75-93fa-454ff6acd66c" width="500" />
<img src="https://github.com/user-attachments/assets/8282c9d8-505d-4be3-b017-06ab2273639d" width="500" />

---
