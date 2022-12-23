#adapted from https://docs.pozyx.io/enterprise/logging-data-from-the-mqtt-stream
import paho.mqtt.client as mqtt
import time
import sys

HOST = 'localhost'
PORT = 1883
TOPIC = 'tags'
DURATION = 300 #duration of data collection in seconds

def on_message(client, userdata, message):
  print(message.payload.decode())  # 'utf-8'

client = mqtt.Client()  # create new instance
client.connect(HOST, port=PORT)  # connect to host
client.on_message = on_message  # attach function to callback
client.loop_start()  # start the loop
client.subscribe(TOPIC)  # subscribe to topic
time.sleep(DURATION)  # wait for duration seconds
client.disconnect()  # disconnect
