# DataLake-Backend

# Prerequisites
Run : pip install -r requirements.txt

# Configuring MinIO (Data Storage)

## Install the MinIO Server

- Download the MinIO executable from the following URL: https://dl.min.io/server/minio/release/windows-amd64/minio.exe   
- Keep this in C Drive.

## Launch the minio server

- In PowerShell or the Command Prompt, navigate to the location of the executable or add the path of the minio.exe file to the system $PATH.  
- Run **.\minio.exe server C:\minio --console-address :9001**  
- Replace C:\minio with location of the executable

## Connect your Browser to the MinIO Server

1. Access the MinIO Console by going to a browser and going to http://127.0.0.1:9001

2. Log in to the Console with the RootUser and RootPass user credentials displayed in the output. These default to minioadmin | minioadmin.

## Use MinIO
### Create buckets
1. Go to buckets in left pannel
2. Create 3 buckets *raw, processed, refine*

### Upload files to bucket
- Run upload.py to upload json files

### Retrieve files from bucket
- Run retrieve.py to download files from buckets

### Configuring MinIO for querying data
- MinIO itself is primarily an object storage solution, it can be used with various tools and frameworks to enable querying capabilities.

###### For more information, read https://min.io/docs/minio/windows/index.html



# Install & Configure Apache Spark (Data Processing)


### Step 1: Download Spark
* Go to https://spark.apache.org/downloads.html
* Choose Pre-built for Apache Hadoop 3.3+ and download Spark 3.x (compatible with Hadoop 3.x)
* Extract it to a directory in C drive (C:\spark-3.5.0-bin-hadoop3) 

### Download Hadoop
* Go to https://hadoop.apache.org/releases.html and Download the latest Hadoop binary release 
* Extract it to a directory (C drive or D drive)

### Install Java (JDK 8 or Higher)
* Check if you have it installed : java -version
* If not Install JDK 8 or higher


### Step 2: Set Up Environment Variables
Add Spark, Hadoop and JAVA to your system environment variables 

##### System Properties → Advanced → Environment Variables → click New and add followings 

* SPARK_HOME → C:\spark-3.5.0-bin-hadoop3 (your_spark_path)
* HADOOP_HOME → C:\spark-3.5.0-bin-hadoop3 (your_hadoop_path)
* JAVA_HOME = C:\Program Files\Java\jdk-17 (your_java_path, if not already set)

##### Add to PATH
* Add %SPARK_HOME%\bin to PATH in system variables
* Add %JAVA_HOME%\bin to the PATH


### Step 3: Verify Installation
- Run following in cmd or sh
spark-shell
hadoop version
java -version


### Step 4: Add Required Dependencies
- Spark needs the Hadoop-AWS and AWS SDK libraries to interact with S3-compatible storage like MinIO.  
- Check if the following JARs are present in <your_Spark_path>/jars. Apache Spark does not include them by default.

* hadoop-aws-<version>.jar

* aws-java-sdk-bundle-<version>.jar

If they are missing, you need to download them from MVN repository and place them in the <your_Spark_path>/jars directory.


##### 1. Find Compatible hadoop-aws and aws-java-sdk-bundle Versions
- First find spark compatible hadoop-aws and aws-jsb versions : https://docs.qubole.com/en/latest/user-guide/engines/spark/spark-supportability.html  
or  
- Official Spark-Hadoop Compatibility: https://spark.apache.org/docs/latest/

##### 2. Download the compatible JARs
##### Hadoop-AWS JAR:

- Maven Repository: https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws  
- Find compatibility version from the list and download it.  
- Place it in the <your_spark_path>/jars directory.  

##### AWS SDK JAR:

- Maven Repository: https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle
- Find compatibility version from the list and download it.
- Place it in the <your_Spark_path>/jars directory.


### Step 5: Run the Codes
- Ensure raw bucket has json files.  
- Run clean_data.py   
- Run Filter_low_confidence_data.py   
- Use read_parquet_file.py to get details of parquet files    
