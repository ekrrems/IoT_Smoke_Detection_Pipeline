from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as func
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import matplotlib.pyplot as plt
import seaborn as sns


# Create a SparkSession
spark = SparkSession.builder.appName("Detector IOT Reader").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("UTC", IntegerType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("TVOC", IntegerType(), True),
    StructField("ECO2", IntegerType(), True),
    StructField("Raw_H2", IntegerType(), True),
    StructField("Raw_Ethanol", IntegerType(), True),
    StructField("Pressure", DoubleType(), True),
    StructField("PM1", DoubleType(), True),
    StructField("PM2", DoubleType(), True),
    StructField("NC0", DoubleType(), True),
    StructField("NC1", DoubleType(), True),
    StructField("NC2", DoubleType(), True),
    StructField("CNT", IntegerType(), True),
    StructField("Fire_Alarm", DoubleType(), True)
])

# Read the CSV data and drop the 'index' column
smoke_data = spark.read.option("header", "true").schema(schema).csv("smoke_detection_iot.csv").drop("index")

# Print the DataFrame schema
smoke_data.printSchema()

# Get column names and shape of the dataset
col_names = smoke_data.columns
row_count = smoke_data.count()
col_count = len(col_names)
print(f"Shape of The Dataset: ({row_count}, {col_count})")

# Glimpse of the DataFrame
smoke_data.show(5)

# Summary of the dataset
data_summary = smoke_data.describe()
data_summary.show()

# Check unique values in the 'Fire_Alarm' column
smoke_data.select("Fire_Alarm").distinct().show()


# Plot Temperature Histograms
def plot_histograms(column):
    grouped_data = smoke_data.groupBy(column, "Fire_Alarm").count()
    histogram_data = grouped_data.toPandas()
    hue_categories = histogram_data["Fire_Alarm"].unique()

    plt.figure(figsize=(14, 8))

    plt.subplot(2, 1, 2)
    for hue_category in hue_categories:
        data_subset = histogram_data[histogram_data["Fire_Alarm"] == hue_category]
        plt.bar(data_subset[column], data_subset["count"], label=hue_category)

    plt.xlabel(f"{column}")
    plt.ylabel("Count")
    plt.title(f"Overall {column} Histogram")
    plt.legend()

    # For Triggering Fire Alarm
    plt.subplot(2, 2, 2)
    triggering_alarm_data = grouped_data.filter(grouped_data["Fire_Alarm"] == 1).toPandas()
    plt.bar(triggering_alarm_data[column], triggering_alarm_data["count"])

    plt.xlabel(f"{column}")
    plt.ylabel("Count")
    plt.title(f"For Triggering Fire Alarm")

    # For Not Triggering Fire Alarm
    plt.subplot(2, 2, 1)
    triggering_alarm_data = grouped_data.filter(grouped_data["Fire_Alarm"] == 0).toPandas()
    plt.bar(triggering_alarm_data[column], triggering_alarm_data["count"])

    plt.xlabel(f"{column}")
    plt.ylabel("Count")
    plt.title("For Not Triggering Fire Alarm")
    plt.show()

#plot_histograms("Temperature")

# Plot Correlation Heatmap
corr_matrix = smoke_data.toPandas().corr()
sns.heatmap(corr_matrix, cmap='coolwarm', center=0, fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
#plt.show() 

# Check the balance of target variables
category_counts = smoke_data.groupBy("Fire_Alarm").count()
category_counts.show()


# Model Creation and Performance Evaluation

# Remove the 'Fire_Alarm' column from the list of feature columns
col_names.remove("Fire_Alarm")

# Assemble the features into a single vector column
vector_assembler = VectorAssembler(inputCols=col_names, outputCol="features")
df = vector_assembler.transform(smoke_data)

# Split the dataset into training and testing sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Create a Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="Fire_Alarm")
lr_model = lr.fit(train_df)

# Make predictions on the testing data
lr_pred = lr_model.transform(test_df)

# Function to evaluate the model's performance
def evaluate_model(predictions):
    # Confusion Matrix
    prediction_and_labels = predictions.select("prediction", "Fire_Alarm").rdd
    metrics = MulticlassMetrics(prediction_and_labels)
    confusion_matrix = metrics.confusionMatrix()
    print(f"Confusion Matrix:\t {confusion_matrix}")

    # Classification Report
    labels = predictions.select("Fire_Alarm").distinct().rdd.map(lambda x: x[0]).collect()
    for label in sorted(labels):
        precision = metrics.precision(label)
        recall = metrics.recall(label)
        f1_score = metrics.fMeasure(label)
        print(f"Class {label}\t Precision: {precision:.4f}\t Recall: {recall:.4f}\t F1-Score: {f1_score:.4f}")

# Call the evaluate_model function on the logistic regression predictions
evaluate_model(lr_pred)

# Save the model
lr_model.save("alarm_prediction_models")

# Stop the SparkSession
spark.stop()