import sys
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QHBoxLayout
import pandas as pd 
from confluent_kafka import Producer
import json
import random

app = QApplication(sys.argv)

class GeneratorGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.data = pd.read_csv("smoke_detection_iot.csv")
        self.producer_config = {
            "bootstrap.servers": "localhost:9092",
            "client.id": "kafka-producer"
            }
        self.producer = Producer(self.producer_config)


    def initUI(self):
        self.setGeometry(100, 100, 300, 200)
        self.setWindowTitle("Generate Button Example")

        layout = QHBoxLayout()  # Create a horizontal layout

        self.generate_button = QPushButton("Generate", self)
        layout.addWidget(self.generate_button)  # Add the button to the layout

        self.setLayout(layout)  # Set the layout for the QWidget

        self.generate_button.clicked.connect(self.generate_button_clicked)


    def generate_button_clicked(self):
        print("Data is sent to Spark Streaming")
        self.kafka_producer()


    def generate_data(self):
        
        data = {
            "UTC": random.randint(self.data["UTC"].min(), self.data["UTC"].max()),
            "Temperature": random.uniform(self.data["Temperature"].min(), self.data["Temperature"].max()),
            "Humidity": random.uniform(self.data["Humidity"].min(), self.data["Humidity"].max()),
            "TVOC": random.randint(self.data["TVOC"].min(), self.data["TVOC"].max()),
            "ECO2": random.randint(self.data["eCO2"].min(), self.data["eCO2"].max()),
            "Raw_H2": random.randint(self.data["Raw_H2"].min(), self.data["Raw_H2"].max()),
            "Raw_Ethanol": random.randint(self.data["Raw_Ethanol"].min(), self.data["Raw_Ethanol"].max()),
            "Pressure": random.uniform(self.data["Pressure"].min(), self.data["Pressure"].max()),
            "PM1": random.uniform(self.data["PM1"].min(), self.data["PM1"].max()),
            "PM2": random.uniform(self.data["PM2"].min(), self.data["PM2"].max()),
            "NC0": random.uniform(self.data["NC0"].min(), self.data["NC0"].max()),
            "NC1": random.uniform(self.data["NC1"].min(), self.data["NC1"].max()),
            "NC2": random.uniform(self.data["NC2"].min(), self.data["NC2"].max()),
            "CNT": random.randint(self.data["CNT"].min(), self.data["CNT"].max())
        }

        json_data = json.dumps(data)

        return json_data
    

    def kafka_producer(self):
        self.producer.produce('kafka-spark-topic', self.generate_data().encode('utf-8'))
        self.producer.flush()


window = GeneratorGUI()
window.show()
sys.exit(app.exec_())