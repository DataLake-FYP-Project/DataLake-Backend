$root = Split-Path -Parent $MyInvocation.MyCommand.Definition

# Run people_service
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Create_Json_Data\people_service'; python main.py"

# Run vehicle_service
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Create_Json_Data\vehicle_service'; python main.py"

# Run safety_service
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Create_Json_Data\safety_service'; python main.py"

# Run pose_service
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Create_Json_Data\pose_service'; python main.py"

# Run Send_Data_To_DataLake
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Send_Data_To_DataLake'; python main.py"

# Run Test_Frontend (Streamlit)
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\Test_Frontend'; python -m streamlit run app.py"
