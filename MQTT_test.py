import random
import time
import json
from datetime import datetime
from datetime import time as tm
from paho.mqtt import client as mqtt_client

from threading import Thread 
from functools import partial

import sqlite3
conn=sqlite3.connect('Group2.sqlite')
c=conn.cursor()

#broker = '142.93.220.247'
broker = '192.168.1.222'
port = 1883
topic = "group1"
topicS = "smartports1_liveData"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
ids=None
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
   # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

result=None
def publish(client):
    global ids,result
    msg_count = 0
#----------------------------STORE AND FORWARD--------------------------#
    SFcount=0                                                             #
    SFprevious_status = None                                              #
#----------------------------STORE AND FORWARD--------------------------#
    while True:
        time.sleep(1)
        msg = "ITS a test meaages to test for the value"
        result = client.publish(topic, msg)
        status = result[0]
        #print(status)
#----------------------------STORE AND FORWARD--------------------------#
        #It will check message sent status if the message was not forwarded to MQTT broker it will store those messages into
        #local (SQLite) DataBase and forwards those messages once the connection was re-established and clear the recordfrom DataBase 
        if status == 0:
            #print(f"Send `{msg_count}` to topic `{topic}`")
            #msg_count += 1
            if status == SFprevious_status:
                pass
            else:
                SFprevious_status=status
                c.execute("SELECT rowid, * FROM jsons") 
                var3=c.fetchall()
                if var3:
                    for i in var3:
                        mag = str(i[2])
                        ids=str(i[0])
                        result = client.publish(topic, mag)
                        c.execute("DELETE from jsons WHERE rowid=("+ids+")")
                        conn.commit()
        else:
            SFprevious_status=status
            print(f"Failed to send message to topic {topic}")
            data=(SFcount,str(msg))
            c.execute("INSERT INTO jsons VALUES (?,?)",data)
            conn.commit()
            SFcount+=1
#----------------------------STORE AND FORWARD--------------------------#
        time.sleep(5)
        


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()
