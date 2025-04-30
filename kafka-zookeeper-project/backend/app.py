""" This is main kafka producer file... """

# Packages
import json  # ✅ Import JSON module
import time
import random
from flask import Flask, jsonify
from confluent_kafka import Producer

# Constant file
from base import constants


# Initialization Flask app
app = Flask(__name__)


# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': constants.KAFKA_BROKER})


@app.route('/send_log')
def send_log():
    log_data = {
        "timestamp": time.strftime("%H:%M:%S"),
        "level": random.choice(["INFO", "WARN", "ERROR"]),
        "message": random.choice(["User logged in", "Database error", "File uploaded"])
    }
    
    # ✅ Fix: Convert dict to JSON string before sending
    producer.produce(constants.KAFKA_TOPIC_NAME, key = "log", value = json.dumps(log_data))  
    producer.flush()
    
    return jsonify({"status": "Log sent", "log": log_data})


if __name__ == '__main__':
    app.run(debug = True, port = 5001)
