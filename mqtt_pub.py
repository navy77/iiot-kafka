import paho.mqtt.client as mqtt
import time
import json
import random
import threading

broker_address = "192.168.0.179" 
client = mqtt.Client()
client.connect(broker_address)
data_topic = "data/mic/demo1/kf1"
status_topic = "status/mic/demo/kf1"
alarm_topic = "alarm/mic/demo/kf1"

def publish_messages_data(topic_num,delay_time,msg_num,pub_loop,init):
    message_data={}

    for i in range(init,pub_loop+init):
        for j in range(1, topic_num+1):  
            topic = data_topic
            for k in range(1,msg_num+1):
                key = f"data{k}"
                message_data[key] = i
                # message_data[key] = random.randint(10000,50000)
            client.publish(topic, json.dumps(message_data))
            # print(json.dumps(message_data))
        time.sleep(delay_time)  

def publish_messages_status(topic_num,delay_time,pub_loop,init):
    message_status={}
    for i in range(init,pub_loop+init):
        for j in range(1, topic_num+1):  
            topic = f"{status_topic}{j}"
            key = f"status"
            message_status[key]=f"status-{i}"
            client.publish(topic, json.dumps(message_status))
            print(json.dumps(message_status))
        time.sleep(delay_time) 

def publish_messages_alarm(topic_num,delay_time,pub_loop,init):
    message_alarm={}
    for i in range(init,pub_loop+init):
        for j in range(1, topic_num+1):  
            topic = f"{alarm_topic}{j}"
            key = f"status"
            message_alarm[key]=f"alarm-{i}"
            client.publish(topic, json.dumps(message_alarm))
            print(json.dumps(message_alarm))
        time.sleep(delay_time) 

def run():
    thread1 = threading.Thread(target=publish_messages_data, args=(1, 1, 5, 5, 1))
    thread2 = threading.Thread(target=publish_messages_status, args=(1, 1, 5, 1))
    thread3 = threading.Thread(target=publish_messages_alarm, args=(1, 1, 5, 1))
    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()

if __name__ == "__main__":
    try:
        # run()

        publish_messages_data(100,0.01,20,10,1)
        # publish_messages_status(1,1,5,1)
        # publish_messages_alarm(1,1,5,1)
    finally:
        client.disconnect()
        print("Disconnected from MQTT broker.")