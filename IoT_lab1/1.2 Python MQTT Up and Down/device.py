import random
import time
import json
import paho.mqtt.client as mqtt
import datetime

THE_BROKER = 'broker.hivemq.com'
topic = "h21ledom/devices/node1/up"

def on_connect(client, userdata, flags, rc):
    print(f'Flags: {flags}, return code: {str(rc)}')
    client.subscribe("h21ledom/devices/node1/down")
    print(f'Subscribed to topic: h21ledom/devices/node1/down')
    print('Sending And Waiting for messages...')

def on_message(client, userdata, msg):
    print(f'\nReceived topic: {msg.topic} with payload: {msg.payload}, at subscriber\'s local time: {datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f")[:-4]}')

def connect_mqtt():
    print(f'Connecting to broker: {THE_BROKER}')
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(THE_BROKER, port=1883)
    client.loop_start()
    print(f'[+] Connected to: {THE_BROKER}, port: 1883')
    return client

def publish_sensor_data(client):
    counter = 0
    while True:
        time.sleep(5)
        sensor_data = {
            "app_id": "h21ledom",
            "dev_id": "node1",
            "port/channel": random.randint(1, 10),
            "rssi": f"-{random.randint(30, 60)}",
            "snr": random.randint(0, 30),
            "sf": "SF7BW125",
            "C_F": "C",
            "temperature": random.randint(20, 30),
            "time": datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S.%f")[:-4],
            "message id/counter": counter
        }
        payload = json.dumps(sensor_data)

        print(f'\nSensor data: {sensor_data["temperature"]}° C at time: {sensor_data["time"]}')

        result = client.publish(topic, payload)
        status = "Publishing" if result.rc == mqtt.MQTT_ERR_SUCCESS else "Failed to send"
        print(f"{status} to topic: {topic}, JSON payload: {payload}")
        counter += 1


if __name__ == "__main__":
    mqtt_client = connect_mqtt()
    publish_sensor_data(mqtt_client)
