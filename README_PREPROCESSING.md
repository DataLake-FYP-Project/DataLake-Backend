## Spark Preprocessing 

### Prerequisites
- Python 3.7+
- Java 8 or 11 (required for Spark)
- Apache Spark (with PySpark - refer MinIO_Spark_Setup.md)
- MinIO server running and accessible (refer MinIO_Spark_Setup.md)
- Python packages listed in requirements.txt

### Installation Steps
1. Clone the repository
```bash
git clone <repository-url>
cd <repository-folder>
```
2. Install Python dependencies
```bash
pip install -r requirements.txt
```
3. Set up environment variables
* Create a .env file in the project root
* Add your MinIO and Spark configurations 

### Running the Project
1. Start MinIO server
- Ensure MinIO is running and accessible at the endpoint specified.
2. Prepare your input data
- Upload your JSON detection files to MinIO
3. Run the main processor
```bash
python main.py
```

### Notes
##### Refer MinIO_Spark_Setup.md for Setup Guidelines

##### MinIO Bucket Structure
* raw
* processed 
* refine

##### Preprocessing Techniques Used 
* Basic preprocessing - Handle null values and timestamps, Filter missing values, Clean string values, Schema validation

* Advanced preprocessing - Order JSON by Tracking id, Get count, maximum, minimum, average values
