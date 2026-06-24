import paho.mqtt.client as mqtt
import time
import sys

received = False

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT broker successfully.")
        client.subscribe("sensor/temp")
        print("Subscribed to 'sensor/temp'")
    else:
        print(f"Failed to connect: {reason_code}")
        sys.exit(1)

def on_message(client, userdata, msg):
    global received
    print(f"Received message: '{msg.payload.decode()}' on topic '{msg.topic}'")
    received = True
    client.disconnect()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "python_test_client")
client.on_connect = on_connect
client.on_message = on_message

try:
    print("Connecting to localhost:1883...")
    client.connect("127.0.0.1", 1883, 60)
except Exception as e:
    print(f"Connection error: {e}")
    sys.exit(1)

client.loop_start()

time.sleep(1)
print("Publishing '42.5' to 'sensor/temp'")
client.publish("sensor/temp", "42.5", qos=1)

time.sleep(2)
client.loop_stop()

if received:
    print("Test passed successfully!")
    sys.exit(0)
else:
    print("Test failed: No message received.")
    sys.exit(1)
