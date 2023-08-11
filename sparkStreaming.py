from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
import psycopg2
from datetime import datetime

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaToSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read streaming data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "kafka-spark-topic").load()

# Convert the 'value' column to a string
df = df.selectExpr("CAST(value AS STRING)")
data = df.select("value")

# Define schema for the JSON data
schema = "UTC long, Temperature double, Humidity double, TVOC long, ECO2 long, Raw_H2 long, Raw_Ethanol long, Pressure double, PM1 double, PM2 double, NC0 double, NC1 double, NC2 double, CNT long"

# Parse JSON data and select fields
parsed_df = df.selectExpr("value", "from_json(value, '{}') AS data".format(schema))
expanded_df = parsed_df.selectExpr("data.*")
col_names = expanded_df.columns

# Assemble features
assembler = VectorAssembler(inputCols=list(col_names), outputCol="features")
assembled_df = assembler.transform(expanded_df)

# Load the pre-trained logistic regression model
loaded_model = LogisticRegressionModel.load("alarm_prediction_models")

# Make predictions using the model
predictions = loaded_model.transform(assembled_df)

# Define a function to process the batch of data
def process_batch(batch_df, batch_id):
    conn = psycopg2.connect(
        host="your-host",
        port="your-port",
        database="your-database",
        user="your-username",
        password="your-password"
    )

    cursor = conn.cursor()
    
    if batch_df.count() == 0:
        print("Batch is empty")

    else:
        # Extract prediction value
        value_row = batch_df.select("prediction").first()
        value = value_row["prediction"]

        # Insert data into PostgreSQL based on prediction
        if value == 1.0:
            insert_query = f"INSERT INTO smoke_detection (fire, date_time) VALUES ('Fire Detected', '{datetime.now()}')"
            cursor.execute(insert_query)

        else:
            print("Under control")
    
    conn.commit()
    cursor.close()
    conn.close()

# Start the streaming query
query = predictions.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# Stop SparkSession
spark.stop()