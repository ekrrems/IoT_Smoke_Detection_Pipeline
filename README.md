# IOT Smoke Detection Pipeline

Welcome to the IoT Smoke Detection Pipeline Project! This project leverages data engineering and machine learning to detect smoke and fire hazards in real time using Internet of Things (IoT) sensors. The pipeline predicts the presence of smoke or fire based on environmental data collected from sensors.

## Overview
This project uses:

* PyQt5
* Apache Kafka
* Apacha Spark, Spark Streaming
* PostgreSQL

This project showcases the following components:

- Data Collection: Sensors capture real-time environmental data including temperature, humidity, and gases. (A Qt5 GUI created to act like a sensor data generator)
- Data Ingestion: Kafka streams efficiently handle data flow.
- Data Processing: PySpark processes, assembles, and predicts.
- Database Integration: PostgreSQL stores and retrieves results.
- Machine Learning: A pre-trained Logistic Regression model predicts smoke and fire hazards.

## Getting Started

Follow these steps to get started with the project:

1. Clone the repository:
   ```sh
   git clone https://github.com/ekrrems/IOT_Smoke_Detection_Pipeline
   ```

2. Set up your environment:
* Install required dependencies (if any).
* Configure Kafka and PostgreSQL (update details in code).

3. Run the project:
* Execute the "ML_model_creation" script to train a Logistic Regression model  
* Execute the "sparkStreaming" and iot_signal_generator" scripts to start the data pipeline and prediction.

## Results
* Upon smoke or fire detection, the system triggers alerts and appropriate actions. Saves the signal date in PostgreSQL database. Real-time analytics enhance safety protocols and prevent disasters.

## Licence
This project is licensed under the MIT License.
