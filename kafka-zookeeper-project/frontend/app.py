""" This is main kafka consumer file... """

# Packages
import json
import time
import threading
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from confluent_kafka import Consumer

# Constant file
from base import constants


# Initialization Flask app
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': constants.KAFKA_BROKER, 
    'group.id': 'log_group', # [This is require to use of groyp of one type of consumers]
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.instance.id': 'log_consumer'  # Add a fixed instance ID [This require if you have multiple consumers]
}


# Kafka Configuration
consumer = Consumer(consumer_config)
consumer.subscribe([constants.KAFKA_TOPIC_NAME])


def emit_log(parsed_log):
    """ Emit logs via WebSockets in a proper context """

    print(f"🚀 Attempting WebSocket event emit: {parsed_log}")
    socketio.emit("log_event", parsed_log, namespace="/")
    print(f"✅ Emit function executed for: {parsed_log}")


def consume_logs():
    """ Continuously poll Kafka and emit logs via WebSockets """

    print("Starting Kafka consumer...")

    while True:
        msg = consumer.poll(10.0)

        if msg is None:
            print("No messages yet...")
            socketio.sleep(1)  # ✅ Use socketio.sleep() instead of time.sleep()
            continue

        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        try:
            log_data = msg.value().decode("utf-8")
            parsed_log = json.loads(log_data)
            print(f"📥 Received Kafka Log: {parsed_log}")

            # ✅ Emit inside a background task to avoid threading issues
            socketio.start_background_task(emit_log, parsed_log)

        except json.JSONDecodeError as e:
            print(f"❌ JSON Decode Error: {e}")

        consumer.commit()  # Ensure offset is committed


@app.route('/')
def home():
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@socketio.on('connect')
def handle_connect():
    print("Client connected!")



if __name__ == '__main__':
    # ✅ Start Kafka consumer as a separate thread
    threading.Thread(target=consume_logs, daemon=True).start()

    # ✅ Run Flask-SocketIO
    socketio.run(app, debug=True, port=5002, allow_unsafe_werkzeug=True)
