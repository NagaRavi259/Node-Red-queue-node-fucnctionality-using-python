import random
import time
import json
from datetime import datetime
from datetime import time as tm
from paho.mqtt import client as mqtt_client


import sqlite3
conn=sqlite3.connect('Group2.sqlite')
c=conn.cursor()
#c.execute("""CREATE TABLE jsons 
#                        (Sno INTEGER,
#                        json text
#                        )
#                    """)


broker = '192.168.1.222'
#broker = 'localhost'
print(broker)
port = 1883
topic = "test_topic1"
topicS = "test_topic2"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'


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
    
Feeder_load_v=[0.5 , 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.6 ,
       0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.7 , 0.71,
       0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.8 , 0.81, 0.82,
       0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.9 , 0.91, 0.92, 0.93,
       0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.  , 1.01, 1.02, 1.03, 1.04,
       1.05, 1.06, 1.07, 1.08, 1.09, 1.1 , 1.11, 1.12, 1.13, 1.14, 1.15,
       1.16, 1.17, 1.18, 1.19, 1.2 , 1.21, 1.22, 1.23, 1.24, 1.25, 1.26,
       1.27, 1.28, 1.29, 1.3 , 1.31, 1.32, 1.33, 1.34, 1.35, 1.36, 1.37,
       1.38, 1.39, 1.4 , 1.41, 1.42, 1.43, 1.44, 1.45, 1.46, 1.47, 1.48,
       1.49, 1.5 , 1.51, 1.52, 1.53, 1.54, 1.55, 1.56, 1.57, 1.58, 1.59,
       1.6 , 1.61, 1.62, 1.63, 1.64, 1.65, 1.66, 1.67, 1.68, 1.69, 1.7 ,
       1.71, 1.72, 1.73, 1.74, 1.75, 1.76, 1.77, 1.78, 1.79, 1.8 , 1.81,
       1.82, 1.83, 1.84, 1.85, 1.86, 1.87, 1.88, 1.89, 1.9 , 1.91, 1.92,
       1.93, 1.94, 1.95, 1.96, 1.97, 1.98, 1.99]
Belt_Speed_v=[2. ,  2.5,  3. ,  3.5,  4. ,  4.5,  5. ,  5.5,  6. ,  6.5,  7. ,
       7.5,  8. ,  8.5,  9. ,  9.5, 10. , 10.5, 11. , 11.5, 12. , 12.5,
       13. , 13.5, 14. , 14.5, 15. , 15.5, 16. , 16.5, 17. , 17.5, 18. ,
       18.5, 19. , 19.5]
Meterial_Flow_Rate_v=[0.05, 0.06, 0.07, 0.08, 0.09, 0.1 , 0.11, 0.12, 0.13, 0.14, 0.15,
                      0.16, 0.17, 0.18, 0.19, 0.2 , 0.21, 0.22, 0.23, 0.24]
Current_R_v=[300, 303, 306, 309, 312, 315, 318, 321, 324, 327, 330, 333, 336,
       339, 342, 345, 348, 351, 354, 357, 360, 363, 366, 369, 372, 375,
       378, 381, 384, 387, 390, 393, 396, 399]
Current_Y_v=[300, 303, 306, 309, 312, 315, 318, 321, 324, 327, 330, 333, 336,
       339, 342, 345, 348, 351, 354, 357, 360, 363, 366, 369, 372, 375,
       378, 381, 384, 387, 390, 393, 396, 399]
Current_B_v=[300, 303, 306, 309, 312, 315, 318, 321, 324, 327, 330, 333, 336,
       339, 342, 345, 348, 351, 354, 357, 360, 363, 366, 369, 372, 375,
       378, 381, 384, 387, 390, 393, 396, 399]
Voltage_R_N_v=[2000, 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100,
       2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190, 2200, 2210,
       2220, 2230, 2240, 2250, 2260, 2270, 2280, 2290, 2300, 2310, 2320,
       2330, 2340, 2350, 2360, 2370, 2380, 2390, 2400, 2410, 2420, 2430,
       2440, 2450, 2460, 2470, 2480, 2490, 2500, 2510, 2520, 2530, 2540,
       2550, 2560, 2570, 2580, 2590, 2600, 2610, 2620, 2630, 2640, 2650,
       2660, 2670, 2680, 2690, 2700, 2710, 2720, 2730, 2740, 2750, 2760,
       2770, 2780, 2790, 2800, 2810, 2820, 2830, 2840, 2850, 2860, 2870,
       2880, 2890, 2900, 2910, 2920, 2930, 2940, 2950, 2960, 2970, 2980,
       2990]
Voltage_Y_N_v=[2000, 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100,
       2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190, 2200, 2210,
       2220, 2230, 2240, 2250, 2260, 2270, 2280, 2290, 2300, 2310, 2320,
       2330, 2340, 2350, 2360, 2370, 2380, 2390, 2400, 2410, 2420, 2430,
       2440, 2450, 2460, 2470, 2480, 2490, 2500, 2510, 2520, 2530, 2540,
       2550, 2560, 2570, 2580, 2590, 2600, 2610, 2620, 2630, 2640, 2650,
       2660, 2670, 2680, 2690, 2700, 2710, 2720, 2730, 2740, 2750, 2760,
       2770, 2780, 2790, 2800, 2810, 2820, 2830, 2840, 2850, 2860, 2870,
       2880, 2890, 2900, 2910, 2920, 2930, 2940, 2950, 2960, 2970, 2980,
       2990]
Voltage_B_N_v=[2000, 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100,
       2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190, 2200, 2210,
       2220, 2230, 2240, 2250, 2260, 2270, 2280, 2290, 2300, 2310, 2320,
       2330, 2340, 2350, 2360, 2370, 2380, 2390, 2400, 2410, 2420, 2430,
       2440, 2450, 2460, 2470, 2480, 2490, 2500, 2510, 2520, 2530, 2540,
       2550, 2560, 2570, 2580, 2590, 2600, 2610, 2620, 2630, 2640, 2650,
       2660, 2670, 2680, 2690, 2700, 2710, 2720, 2730, 2740, 2750, 2760,
       2770, 2780, 2790, 2800, 2810, 2820, 2830, 2840, 2850, 2860, 2870,
       2880, 2890, 2900, 2910, 2920, 2930, 2940, 2950, 2960, 2970, 2980,
       2990]
Motor_Vibration_v=[-20. , -19.5, -19. , -18.5, -18. , -17.5, -17. , -16.5, -16. ,
       -15.5, -15. , -14.5, -14. , -13.5, -13. , -12.5, -12. , -11.5,
       -11. , -10.5, -10. ,  -9.5,  -9. ,  -8.5,  -8. ,  -7.5,  -7. ,
       -6.5,  -6. ,  -5.5,  -5. ,  -4.5,  -4. ,  -3.5,  -3. ,  -2.5,
       -2. ,  -1.5,  -1. ,  -0.5,   0. ,   0.5,   1. ,   1.5,   2. ,
       2.5,   3. ,   3.5,   4. ,   4.5,   5. ,   5.5,   6. ,   6.5,
       7. ,   7.5,   8. ,   8.5,   9. ,   9.5,  10. ,  10.5,  11. ,
       11.5,  12. ,  12.5,  13. ,  13.5,  14. ,  14.5,  15. ,  15.5,
       16. ,  16.5,  17. ,  17.5,  18. ,  18.5,  19. ,  19.5]
Motor_Temprature_v=[22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
        32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,
        45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,
        58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,
        71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,
        84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,
        97,  98,  99]
Belt_Torque_v=[2.5,  5. ,  7.5, 10. , 12.5, 15. , 17.5, 20. , 22.5, 25. ,
       27.5, 30. , 32.5, 35. , 37.5, 40. , 42.5, 45. , 47.5, 50. , 52.5,
       55. , 57.5, 60. , 62.5, 65. , 67.5, 70. , 72.5, 75. , 77.5, 80. ,
       82.5, 85. , 87.5, 90. , 92.5, 95. , 97.5]
Belt_Tensile_v=[ 0. ,  0.9,  1.8,  2.7,  3.6,  4.5,  5.4,  6.3,  7.2,  8.1,  9. ,
        9.9, 10.8, 11.7, 12.6, 13.5, 14.4, 15.3, 16.2, 17.1, 18. , 18.9,
       19.8, 20.7, 21.6, 22.5, 23.4, 24.3, 25.2, 26.1, 27. , 27.9, 28.8,
       29.7, 30.6, 31.5, 32.4, 33.3, 34.2, 35.1, 36. , 36.9, 37.8, 38.7,
       39.6]
Active_Energy_v=[1200, 1210, 1220, 1230, 1240, 1250, 1260, 1270, 1280, 1290, 1300,
       1310, 1320, 1330, 1340, 1350, 1360, 1370, 1380, 1390, 1400, 1410,
       1420, 1430, 1440, 1450, 1460, 1470, 1480, 1490]
Ton_Per_Hour_v=[1600, 1610, 1620, 1630, 1640, 1650, 1660, 1670, 1680, 1690, 1700,
       1710, 1720, 1730, 1740, 1750, 1760, 1770, 1780, 1790, 1800, 1810,
       1820, 1830, 1840, 1850, 1860, 1870, 1880, 1890, 1900, 1910, 1920,
       1930, 1940, 1950, 1960, 1970, 1980, 1990]
Stationary_Speed_v=[ 3. ,  3.5,  4. ,  4.5,  5. ,  5.5,  6. ,  6.5,  7. ,  7.5,  8. ,
        8.5,  9. ,  9.5, 10. , 10.5, 11. , 11.5, 12. , 12.5, 13. , 13.5,
       14. , 14.5]
Transient_Speed_v=[ 3. ,  3.5,  4. ,  4.5,  5. ,  5.5,  6. ,  6.5,  7. ,  7.5,  8. ,
        8.5,  9. ,  9.5, 10. , 10.5, 11. , 11.5, 12. , 12.5, 13. , 13.5,
       14. , 14.5]
Right_Sensor_1_v=[2,  4,  6,  8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32,
       34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66,
       68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]
Left_Sensor_1_v=[2,  4,  6,  8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32,
       34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66,
       68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]
Right_Sensor_2_v=[2,  4,  6,  8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32,
       34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66,
       68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]
Left_Sensor_2_v=[2,  4,  6,  8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32,
       34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66,
       68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]
Sensor_Speed_01_v=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1. , 1.1, 1.2,
       1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2. , 2.1, 2.2, 2.3, 2.4, 2.5,
       2.6, 2.7, 2.8, 2.9, 3. , 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8,
       3.9, 4. , 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5. , 5.1,
       5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9]
Sensor_Speed_02_v=[0. , 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1. , 1.1, 1.2,
       1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2. , 2.1, 2.2, 2.3, 2.4, 2.5,
       2.6, 2.7, 2.8, 2.9, 3. , 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8,
       3.9, 4. , 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5. , 5.1,
       5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9]
Deviation_Speed_Sensor_01_v=[0. , 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1. , 1.1, 1.2,
       1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2. , 2.1, 2.2, 2.3, 2.4, 2.5,
       2.6, 2.7, 2.8, 2.9, 3. , 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8,
       3.9, 4. , 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5. , 5.1,
       5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9]
Deviation_Speed_Sensor_02_v=[0. , 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1. , 1.1, 1.2,
       1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2. , 2.1, 2.2, 2.3, 2.4, 2.5,
       2.6, 2.7, 2.8, 2.9, 3. , 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8,
       3.9, 4. , 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5. , 5.1,
       5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9]
idle_current=random.randint(5, 10)
idle_current=random.randint(400, 450)
idle_Temprature=35
idle_Active_energy=15
device=""
mag=""

def publish(client):
    global device,mag
    msg_count = 0
    #print("started")
    previous_status =0
    R1_constant = 0
    L1_constant = 0
    R2_constant = 0
    L2_constant = 0
#-----------------------SF-----------------------#
    SFcount=1                                                            #
    SFprevious_status = None  
#-----------------------SF-----------------------#
    while True:
        ti=datetime.now().time()
        #print("in while loop")
        if ((ti>=tm(6,0,0)) & (ti<tm(19,0,0))):
            ####################################----------------Running_Condition-----------#############################################
            start_status=1
            FL=random.choice(Feeder_load_v)
            BS=random.choice(Belt_Speed_v)
            MF=random.choice(Meterial_Flow_Rate_v)
            C_R=random.choice(Current_R_v)
            C_Y=random.choice(Current_Y_v)
            C_B=random.choice(Current_B_v)
            V_R=random.choice(Voltage_R_N_v)
            V_Y=random.choice(Voltage_Y_N_v)
            V_B=random.choice(Voltage_B_N_v)
            MV=random.choice(Motor_Vibration_v)
            MT=random.choice(Motor_Temprature_v)
            BTorque=random.choice(Belt_Torque_v)
            BTensile=random.choice(Belt_Tensile_v)
            AE=random.choice(Active_Energy_v)
            TH=random.choice(Ton_Per_Hour_v)
            R1=random.choice(Right_Sensor_1_v)
            R1_constant=R1
            L1=random.choice(Left_Sensor_1_v)
            L1_constant=L1
            R2=random.choice(Right_Sensor_2_v)
            R2_constant=R2
            L2=random.choice(Left_Sensor_2_v)
            L2_constant=L2
            SS1=random.choice(Sensor_Speed_01_v)
            SS2=random.choice(Sensor_Speed_02_v)
            DS1=random.choice(Deviation_Speed_Sensor_01_v)
            DS2=random.choice(Deviation_Speed_Sensor_02_v)
            SST=random.choice(Stationary_Speed_v)
            TS=random.choice(Transient_Speed_v)
            CA=round(((C_R+C_Y+C_B)/3),2)
            VA=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1+L1)/2),2)
            MA1_constant=MA1
            MA2=round(((R2+L2)/2),2)
            MA2_constant=MA2
            if ((ti>=tm(6,5,0)) & (ti<tm(6,20,0))):
                FL,MFR,TPH=0,0,0
            elif ((ti>=tm(11,30,0)) & (ti<tm(11,15,0))):
                FL,MFR,TPH=0,0,0
            elif ((ti>=tm(15,10,0)) & (ti<tm(15,30,0))):
                FL,MFR,TPH=0,0,0
            
            
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':FL,'Belt_Speed':BS,'Material_Flow_Rate':MF,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA,'Motor_Vibration':MV,'Motor_Temprature':MT,'Belt_Torque':BTorque,'Belt_Speed':BS,'Belt_Tensile':BTensile,'Active_Energy':AE,'Ton_Per_Hour':TH,'Stationary_Speed':SST,'Right_Sensor_1':R1,'Left_Sensor_1':L1,'Transient_Speed':TS,'Right_Sensor_2':R2,'Left_Sensor_2':L2,'Misalignment_1':MA1,'Misalignment_2':MA2,'Sensor_Speed_01':SS1,'Sensor_Speed_02':SS2,'Deviation_Speed_Sensor_01':DS1,'Deviation_Speed_Sensor_02':DS2}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
        
        
        elif ((ti>=tm(19,0,0)) & (ti<tm(19,30,0))):
            ####################################----------------IDLE Condition-----------#############################################
            start_status=0
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            C_R=random.randint(5, 10)
            C_Y=random.randint(5, 10)
            C_B=random.randint(5, 10)
            V_R=random.randint(400, 450)
            V_Y=random.randint(400, 450)
            V_B=random.randint(400, 450)
            CA_IDLE=round(((C_R+C_Y+C_B)/3),2)
            VA_IDLE=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1_constant+L1_constant)/2),2)
            MA1_IDLE=MA1
            MA2=round(((R2_constant+L2_constant)/2),2)
            MA2_IDLE=MA2

            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':0,'Belt_Speed':0,'Material_Flow_Rate':0,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA_IDLE,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA_IDLE,'Motor_Vibration':0,'Motor_Temprature':35,'Belt_Torque':0,'Belt_Speed':0,'Belt_Tensile':0,'Active_Energy':15,'Ton_Per_Hour':0,'Stationary_Speed':0,'Right_Sensor_1':R1_constant,'Left_Sensor_1':L1_constant,'Transient_Speed':0,'Right_Sensor_2':R2_constant,'Left_Sensor_2':L2_constant,'Misalignment_1':MA1_IDLE,'Misalignment_2':MA2_IDLE,'Sensor_Speed_01':0,'Sensor_Speed_02':0,'Deviation_Speed_Sensor_01':0,'Deviation_Speed_Sensor_02':0}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
        elif ((ti>=tm(19,30,0)) & (ti<tm(20,25,0))):
            ####################################----------------Running_Condition-----------#############################################
            start_status=1
            FL=random.choice(Feeder_load_v)
            BS=random.choice(Belt_Speed_v)
            MF=random.choice(Meterial_Flow_Rate_v)
            C_R=random.choice(Current_R_v)
            C_Y=random.choice(Current_Y_v)
            C_B=random.choice(Current_B_v)
            V_R=random.choice(Voltage_R_N_v)
            V_Y=random.choice(Voltage_Y_N_v)
            V_B=random.choice(Voltage_B_N_v)
            MV=random.choice(Motor_Vibration_v)
            MT=random.choice(Motor_Temprature_v)
            BTorque=random.choice(Belt_Torque_v)
            BTensile=random.choice(Belt_Tensile_v)
            AE=random.choice(Active_Energy_v)
            TH=random.choice(Ton_Per_Hour_v)
            R1=random.choice(Right_Sensor_1_v)
            R1_constant=R1
            L1=random.choice(Left_Sensor_1_v)
            L1_constant=L1
            R2=random.choice(Right_Sensor_2_v)
            R2_constant=R2
            L2=random.choice(Left_Sensor_2_v)
            L2_constant=L2
            SS1=random.choice(Sensor_Speed_01_v)
            SS2=random.choice(Sensor_Speed_02_v)
            DS1=random.choice(Deviation_Speed_Sensor_01_v)
            DS2=random.choice(Deviation_Speed_Sensor_02_v)
            SST=random.choice(Stationary_Speed_v)
            TS=random.choice(Transient_Speed_v)
            CA=round(((C_R+C_Y+C_B)/3),2)
            VA=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1+L1)/2),2)
            MA1_constant=MA1
            MA2=round(((R2+L2)/2),2)
            MA2_constant=MA2
            
            if ((ti>=tm(19,36,0)) & (ti<tm(19,46,0))):
                FL,MFR,TPH=0,0,0
            
            
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':FL,'Belt_Speed':BS,'Material_Flow_Rate':MF,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA,'Motor_Vibration':MV,'Motor_Temprature':MT,'Belt_Torque':BTorque,'Belt_Speed':BS,'Belt_Tensile':BTensile,'Active_Energy':AE,'Ton_Per_Hour':TH,'Stationary_Speed':SST,'Right_Sensor_1':R1,'Left_Sensor_1':L1,'Transient_Speed':TS,'Right_Sensor_2':R2,'Left_Sensor_2':L2,'Misalignment_1':MA1,'Misalignment_2':MA2,'Sensor_Speed_01':SS1,'Sensor_Speed_02':SS2,'Deviation_Speed_Sensor_01':DS1,'Deviation_Speed_Sensor_02':DS2}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]    
            trip=True
        elif ((ti>=tm(20,25,0)) & (ti<tm(21,15,0))):
            ####################################----------------IDLE Condition-----------#############################################
            start_status=0
            Conveyor_Trip_Status_v=1
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            C_R=random.randint(5, 10)
            C_Y=random.randint(5, 10)
            C_B=random.randint(5, 10)
            V_R=random.randint(400, 450)
            V_Y=random.randint(400, 450)
            V_B=random.randint(400, 450)
            CA_IDLE=round(((C_R+C_Y+C_B)/3),2)
            VA_IDLE=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1_constant+L1_constant)/2),2)
            MA1_IDLE=MA1
            MA2=round(((R2_constant+L2_constant)/2),2)
            MA2_IDLE=MA2

            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':0,'Belt_Speed':0,'Material_Flow_Rate':0,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA_IDLE,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA_IDLE,'Motor_Vibration':0,'Motor_Temprature':35,'Belt_Torque':0,'Belt_Speed':0,'Belt_Tensile':0,'Active_Energy':15,'Ton_Per_Hour':0,'Stationary_Speed':0,'Right_Sensor_1':R1_constant,'Left_Sensor_1':L1_constant,'Transient_Speed':0,'Right_Sensor_2':R2_constant,'Left_Sensor_2':L2_constant,'Misalignment_1':MA1_IDLE,'Misalignment_2':MA2_IDLE,'Sensor_Speed_01':0,'Sensor_Speed_02':0,'Deviation_Speed_Sensor_01':0,'Deviation_Speed_Sensor_02':0}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
            deviceTrip={'timeStamp':str(ti1),'deviceId':130,'data':{'Conveyor_Trip_Status':1}}
            bTrip=json.dumps(deviceTrip)
            trip_stop=True
            while trip:
                print("Trip_stop = 1")
                result = client.publish(topicS, bTrip)
                trip=False
                ############################---ZSS----############################
                ti=datetime.now()
                ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
                deviceZSS={'timeStamp':str(ti1),'deviceId':130,'data':{'ZSS':1}}
                bZSS=json.dumps(deviceZSS)
                result = client.publish(topicS, bZSS)
                time.sleep(120)
                ti=datetime.now()
                ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
                deviceZSS={'timeStamp':str(ti1),'deviceId':130,'data':{'ZSS':0}}
                bZSS=json.dumps(deviceZSS)
                result = client.publish(topicS, bZSS)
                ############################---ZSS--############################
                
        elif ((ti>=tm(21,15,0)) & (ti<tm(23,59,59))):
            ####################################----------------Running_Condition-----------#############################################

            start_status=1


            while trip_status:
                print("Trip_stop = 0")
                ti=datetime.now()
                ti1=ti.strftime("%Y-%m-%d %H:%M:%S")


                deviceTrip_stop={'timeStamp':str(ti1),'deviceId':130,'data':{'Conveyor_Trip_Status':1}}
                bTrip_stop=json.dumps(deviceTrip_stop)
                trip_stop=True
                result = client.publish(topicS, bTrip_stop)
            Conveyor_Trip_Status_v=0
            FL=random.choice(Feeder_load_v)
            BS=random.choice(Belt_Speed_v)
            MF=random.choice(Meterial_Flow_Rate_v)
            C_R=random.choice(Current_R_v)
            C_Y=random.choice(Current_Y_v)
            C_B=random.choice(Current_B_v)
            V_R=random.choice(Voltage_R_N_v)
            V_Y=random.choice(Voltage_Y_N_v)
            V_B=random.choice(Voltage_B_N_v)
            MV=random.choice(Motor_Vibration_v)
            MT=random.choice(Motor_Temprature_v)
            BTorque=random.choice(Belt_Torque_v)
            BTensile=random.choice(Belt_Tensile_v)
            AE=random.choice(Active_Energy_v)
            TH=random.choice(Ton_Per_Hour_v)
            R1=random.choice(Right_Sensor_1_v)
            R1_constant=R1
            L1=random.choice(Left_Sensor_1_v)
            L1_constant=L1
            R2=random.choice(Right_Sensor_2_v)
            R2_constant=R2
            L2=random.choice(Left_Sensor_2_v)
            L2_constant=L2
            SS1=random.choice(Sensor_Speed_01_v)
            SS2=random.choice(Sensor_Speed_02_v)
            DS1=random.choice(Deviation_Speed_Sensor_01_v)
            DS2=random.choice(Deviation_Speed_Sensor_02_v)
            SST=random.choice(Stationary_Speed_v)
            TS=random.choice(Transient_Speed_v)
            CA=round(((C_R+C_Y+C_B)/3),2)
            VA=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1+L1)/2),2)
            MA1_constant=MA1
            MA2=round(((R2+L2)/2),2)
            MA2_constant=MA2
            
            if ((ti>=tm(23,40,0)) & (ti<tm(23,59,50))):
                FL,MFR,TPH=0,0,0
            
            
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':FL,'Belt_Speed':BS,'Material_Flow_Rate':MF,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA,'Motor_Vibration':MV,'Motor_Temprature':MT,'Belt_Torque':BTorque,'Belt_Speed':BS,'Belt_Tensile':BTensile,'Active_Energy':AE,'Ton_Per_Hour':TH,'Stationary_Speed':SST,'Right_Sensor_1':R1,'Left_Sensor_1':L1,'Transient_Speed':TS,'Right_Sensor_2':R2,'Left_Sensor_2':L2,'Misalignment_1':MA1,'Misalignment_2':MA2,'Sensor_Speed_01':SS1,'Sensor_Speed_02':SS2,'Deviation_Speed_Sensor_01':DS1,'Deviation_Speed_Sensor_02':DS2}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
        elif ((ti>=tm(0,0,0)) & (ti<tm(4,30,0))):
            ####################################----------------IDLE Condition-----------#############################################
            start_status=0
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            C_R=random.randint(5, 10)
            C_Y=random.randint(5, 10)
            C_B=random.randint(5, 10)
            V_R=random.randint(400, 450)
            V_Y=random.randint(400, 450)
            V_B=random.randint(400, 450)
            CA_IDLE=round(((C_R+C_Y+C_B)/3),2)
            VA_IDLE=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1_constant+L1_constant)/2),2)
            MA1_IDLE=MA1
            MA2=round(((R2_constant+L2_constant)/2),2)
            MA2_IDLE=MA2

            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':0,'Belt_Speed':0,'Material_Flow_Rate':0,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA_IDLE,'Voltage_R_N':V_R,'Voltage_Y_ N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA_IDLE,'Motor_Vibration':0,'Motor_Temprature':35,'Belt_Torque':0,'Belt_Speed':0,'Belt_Tensile':0,'Active_Energy':15,'Ton_Per_Hour':0,'Stationary_Speed':0,'Right_Sensor_1':R1_constant,'Left_Sensor_1':L1_constant,'Transient_Speed':0,'Right_Sensor_2':R2_constant,'Left_Sensor_2':L2_constant,'Misalignment_1':MA1_IDLE,'Misalignment_2':MA2_IDLE,'Sensor_Speed_01':0,'Sensor_Speed_02':0,'Deviation_Speed_Sensor_01':0,'Deviation_Speed_Sensor_02':0}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
        elif ((ti>=tm(4,30,0)) & (ti<tm(5,59,0))):
            ####################################----------------Running_Condition-----------#############################################
            start_status=1
            FL=random.choice(Feeder_load_v)
            BS=random.choice(Belt_Speed_v)
            MF=random.choice(Meterial_Flow_Rate_v)
            C_R=random.choice(Current_R_v)
            C_Y=random.choice(Current_Y_v)
            C_B=random.choice(Current_B_v)
            V_R=random.choice(Voltage_R_N_v)
            V_Y=random.choice(Voltage_Y_N_v)
            V_B=random.choice(Voltage_B_N_v)
            MV=random.choice(Motor_Vibration_v)
            MT=random.choice(Motor_Temprature_v)
            BTorque=random.choice(Belt_Torque_v)
            BTensile=random.choice(Belt_Tensile_v)
            AE=random.choice(Active_Energy_v)
            TH=random.choice(Ton_Per_Hour_v)
            R1=random.choice(Right_Sensor_1_v)
            R1_constant=R1
            L1=random.choice(Left_Sensor_1_v)
            L1_constant=L1
            R2=random.choice(Right_Sensor_2_v)
            R2_constant=R2
            L2=random.choice(Left_Sensor_2_v)
            L2_constant=L2
            SS1=random.choice(Sensor_Speed_01_v)
            SS2=random.choice(Sensor_Speed_02_v)
            DS1=random.choice(Deviation_Speed_Sensor_01_v)
            DS2=random.choice(Deviation_Speed_Sensor_02_v)
            SST=random.choice(Stationary_Speed_v)
            TS=random.choice(Transient_Speed_v)
            CA=round(((C_R+C_Y+C_B)/3),2)
            VA=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1+L1)/2),2)
            MA1_constant=MA1
            MA2=round(((R2+L2)/2),2)
            MA2_constant=MA2
            
            if ((ti>=tm(5,23,0)) & (ti<tm(5,34,0))):
                FL,MFR,TPH=0,0,0
            
            
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':FL,'Belt_Speed':BS,'Material_Flow_Rate':MF,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA,'Motor_Vibration':MV,'Motor_Temprature':MT,'Belt_Torque':BTorque,'Belt_Speed':BS,'Belt_Tensile':BTensile,'Active_Energy':AE,'Ton_Per_Hour':TH,'Stationary_Speed':SST,'Right_Sensor_1':R1,'Left_Sensor_1':L1,'Transient_Speed':TS,'Right_Sensor_2':R2,'Left_Sensor_2':L2,'Misalignment_1':MA1,'Misalignment_2':MA2,'Sensor_Speed_01':SS1,'Sensor_Speed_02':SS2,'Deviation_Speed_Sensor_01':DS1,'Deviation_Speed_Sensor_02':DS2}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            status = result[0]
        elif ((ti>=tm(5,59,0)) & (ti<tm(6,0,0))):
            ####################################----------------Running_Condition-----------#############################################
            start_status=1
            FL=random.choice(Feeder_load_v)
            BS=random.choice(Belt_Speed_v)
            MF=random.choice(Meterial_Flow_Rate_v)
            C_R=random.choice(Current_R_v)
            C_Y=random.choice(Current_Y_v)
            C_B=random.choice(Current_B_v)
            V_R=random.choice(Voltage_R_N_v)
            V_Y=random.choice(Voltage_Y_N_v)
            V_B=random.choice(Voltage_B_N_v)
            MV=random.choice(Motor_Vibration_v)
            MT=random.choice(Motor_Temprature_v)
            BTorque=random.choice(Belt_Torque_v)
            BTensile=random.choice(Belt_Tensile_v)
            AE=random.choice(Active_Energy_v)
            TH=random.choice(Ton_Per_Hour_v)
            R1=random.choice(Right_Sensor_1_v)
            R1_constant=R1
            L1=random.choice(Left_Sensor_1_v)
            L1_constant=L1
            R2=random.choice(Right_Sensor_2_v)
            R2_constant=R2
            L2=random.choice(Left_Sensor_2_v)
            L2_constant=L2
            SS1=random.choice(Sensor_Speed_01_v)
            SS2=random.choice(Sensor_Speed_02_v)
            DS1=random.choice(Deviation_Speed_Sensor_01_v)
            DS2=random.choice(Deviation_Speed_Sensor_02_v)
            SST=random.choice(Stationary_Speed_v)
            TS=random.choice(Transient_Speed_v)
            CA=round(((C_R+C_Y+C_B)/3),2)
            VA=round(((V_R+V_Y+V_B)/3),2)
            MA1=round(((R1+L1)/2),2)
            MA1_constant=MA1
            MA2=round(((R2+L2)/2),2)
            MA2_constant=MA2
            
            m=random.randint(0, 25)
            values2592=[[19, 3, 2],[20, 3, 1],[21, 3, 0],[18, 4, 2],[16, 5, 3],[15, 6, 3],[12, 9, 3],
            [15, 9, 0],[14, 10, 0],[13, 11, 0],[13, 8, 3],[13, 11, 0],[14, 10, 0],[14, 10, 0],
            [14, 10, 0],[14, 10, 0],[17, 7, 0],[18, 5, 1],[19, 5, 0],[20, 4, 0],
            [21, 2, 1],[21, 3, 0],[22, 1, 1],[22, 2, 0],[22, 2, 0],[23, 1, 0]]

            Conveyor_Start_Status_v,Belt_Ideal_Time_v,Conveyor_Trip_Status_v=values2592[m][0],values2592[m][1],values2592[m][2]
            
            ti=datetime.now()
            ti1=ti.strftime("%Y-%m-%d %H:%M:%S")
            device={'timeStamp':str(ti1),'deviceId':130,'data':{'Feeder_Load':FL,'Belt_Speed':BS,'Material_Flow_Rate':MF,'Current_R':C_R,'Current_Y':C_Y,'Current_B':C_B,'Active_Power_Total':CA,'Voltage_R_N':V_R,'Voltage_Y_N':V_Y,'Voltage_B_N':V_B,'Voltage_L_N_Avg':VA,'Motor_Vibration':MV,'Motor_Temprature':MT,'Belt_Torque':BTorque,'Belt_Speed':BS,'Belt_Tensile':BTensile,'Active_Energy':AE,'Ton_Per_Hour':TH,'Stationary_Speed':SST,'Right_Sensor_1':R1,'Left_Sensor_1':L1,'Transient_Speed':TS,'Right_Sensor_2':R2,'Left_Sensor_2':L2,'Misalignment_1':MA1,'Misalignment_2':MA2,'Sensor_Speed_01':SS1,'Sensor_Speed_02':SS2,'Deviation_Speed_Sensor_01':DS1,'Deviation_Speed_Sensor_02':DS2}}
            device2={'timeStamp':str(ti1),'deviceId':130,'data':{'Belt_Operating_Time':Conveyor_Start_Status_v,'Belt_Ideal_Time':Belt_Ideal_Time_v,'Belt_Downtime':Conveyor_Trip_Status_v}}
            msg = f"messages: {msg_count}"
            b=json.dumps(device)
            b2=json.dumps(device2)
            output=json.dumps(device)
            output2=json.dumps(device2)
            #print(b)
            #output={'Timestamp':ti,'object':b}
            result = client.publish(topic, b)
            result2 = client.publish(topic, b2)
            status = result[0]
            #
            
        
        if start_status == previous_status:
            pass
        else:
            print(start_status,previous_status)
            previous_status=start_status
            status_tags={'timeStamp':str(ti1),'deviceId':130,'data':{'Conveyor_Start_Status':start_status}}
            BS=json.dumps(status_tags)
            Result_status_tags=client.publish(topicS, BS)
            print("status changed",start_status)

            

        
		# elif ((ti>=tm(14,0,0)) & (ti<tm(15,0,0))):
			# a=9 
		# elif ((ti>=tm(15,0,0)) & (ti<tm(16,0,0))):
			# a=10 
		# elif ((ti>=tm(16,0,0)) & (ti<tm(17,0,0))):
			# a=11 
		# elif ((ti>=tm(17,0,0)) & (ti<tm(18,0,0))):
			# a=12 
            
        if status == 0:
            print(f"Send `{msg_count}` to topic `{topic}`")
            msg_count += 1
            if status == SFprevious_status:
                pass
            else:
                SFprevious_status=status
                c.execute("SELECT rowid, * FROM jsons") 
                var3=c.fetchall()
                if var3:
                    for i in var3:
                        mag = json.dumps(eval(i[2]))
                        ids=str(i[0])
                        result = client.publish(topic, mag)
                        c.execute("DELETE from jsons WHERE rowid=("+ids+")")
                        conn.commit()
        else:
            SFprevious_status=status
            print(f"Failed to send message to topic {topic}")
            data=(SFcount,str(b))
            c.execute("INSERT INTO jsons VALUES (?,?)",data)
            conn.commit()
            SFcount+=1
            
            
        time.sleep(6)
        
        msg_count += 1


if __name__ == '__main__':
    client = connect_mqtt()
    client.loop_start()
    publish(client)
