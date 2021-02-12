#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
import re
from confluent_kafka import Consumer,TopicPartition
import json
import ccloud_lib
months=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
days=[31,28,31,30,31,30,31,31,30,31,30,31]
most_month=[0 for _ in range(13)]
most_day=[0 for _ in range(33)]
id_set=['1776','2218','2220','2223','2224','2226','2227','2228','2231','2232','2233','2235','2237','2238','2239','2240','2241','2242','2243','2244','2245','2246','2247','2248','2249','2250','2251','2262','2263','2264','2265','2266','2267','2268','2269','2270','2271','2272','2273','2274','2275','2276','2277','2278','2279','2280','2281','2282','2283','2284','2285','2286','2287','2288','2289','2290','2291','2292','2293','2294','2401','2402','2403','2404','2091','2092','4001','4001','4002','4003','4004','4005','4006','4007','4008','4009','4010','4011','4012','4013','4014','4015','4016','4017','4018','4019','4020','4021','4022','4023','4024','4025','4026','4027','4028','4029','4030','4031','4032','4033','4034','4035','4036','4037','4038']
keys=set()
if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
       # 'isolation.level':'read_committed',
       # 'transactional.id':'1',
    })
    #H:Consumer Groups
    '''
    consumer2 = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_2',
        'auto.offset.reset': 'earliest',
    })
    consumer3 = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })'''
    # Subscribe to topic
   #F:varify Keys:
    consumer.assign([TopicPartition(topic,5)])
   # consumer.subscribe([topic])
   #consumer2.subscribe([topic])
   #consumer3.subscribe([topic])
    # Process messages
    total_count = 0
    
    try:
       #Read and discards all records in a topic 
       while True:
        for i in range (300):
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error() :
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                record_offset=msg.offset()
                total_count += 1
                str=record_value.decode('utf-8')
                j=json.loads(str)
                if(len(j['EVENT_NO_TRIP'])==0):
                    print("Event trip error!")
                else:
                    even_no_trip=j['EVENT_NO_TRIP']
                print(type(even_no_trip))
                even_no_stop=j['EVENT_NO_STOP']
                print(type(even_no_stop))
                if len(j['OPD_DATE'])==0 or len(j['ACT_TIME'])==0 or len(j['VEHICLE_ID'])==0:
                   print("Joint primary key error!")
                else:
                   joint_key=j['OPD_DATE']+j['ACT_TIME']+j['VEHICLE_ID']
                   if joint_key not in keys:
                       keys.add(joint_key)
                   else:
                       print("Joint key already in set!",joint_key)
                if(len(j['VEHICLE_ID'])==0 or len(j['VEHICLE_ID'])!=4):
                    print("Vehicle id error!")
                else:
                    vehicle_id=j['VEHICLE_ID']
                    if(vehicle_id not in id_set):
                        print("Vehicle id is not in table!Vehicle id is",vehicle_id)
                if(len(j['METERS'])==0):
                    print("Meters error!")
                else:
                    meters=j['METERS']
                if(len(j['ACT_TIME'])==0):
                    print("Act time error!")
                else:
                    act_time=j['ACT_TIME']
                    time=int(act_time)
                    if time not in range(15900,90600):
                        print("Time error!Time is",time)
                if(len(j['VELOCITY'])==0):
                    print("Velocity error!")
                else:
                    velocity=j['VELOCITY']
                if(len(j['DIRECTION'])==0):
                    print("Direction is empty!")
                else:
                    direction=int((j['DIRECTION']))
                    if direction not in (0,359):
                        print("Direction error!Direction is",direction)
                if(len(j['RADIO_QUALITY'])==0):
                    print("Radio quality error!")
                else:
                    radio_quality=j['RADIO_QUALITY']
                if len(j['GPS_LONGITUDE'])==0 or len(j['GPS_LATITUDE'])==0 or len(j['GPS_HDOP'])==0:
                    print("GPS error!")
                else:
                    gps_longitude=j['GPS_LONGITUDE']
                    gps_latitude=j['GPS_LATITUDE']
                    gps_hdop=j['GPS_HDOP']
                hdop=float(gps_hdop)
                if(hdop>10):
                    print("Gps hdop error!hdop is",hdop)

                longitude=float(gps_longitude)
                latitude=float(gps_latitude)
                if longitude>-122.00 or longitude<-122.90:
                    print("Longitude error!Longitude is",longitude)
                if latitude>45.99 or latitude<45.40:
                    print("Latitude error!Latitude is",latitude)
                schedule_deviation=j['SCHEDULE_DEVIATION']
                pattern = r'[-]'
                opd_date=j['OPD_DATE']
                day_mon_year=re.split(pattern,opd_date)
                day=day_mon_year[0]
                mon=day_mon_year[1]
                year=day_mon_year[2]
        
                print(day)
                print(mon)
                print(year)
                if year!='20':
                    year='20'
                if mon not in months:
                    mon=months[most_months.index(max_months.max())]
                else:
                    most_month[months.index(mon)]+=1
                    max_day=days[months.index(mon)]
                day=int(day)
                max
                if (day>max_day):
                    day=days[most_days.index(max_days.max())]
                else:
                    most_day[day]+=1
                print(day)
                print(mon)
                print(year)
                print("Consumer0:Consumed record with key {} and value {}, \
                      and updated total count to {},\
                      the offset is {}"
                      .format(record_key, record_value, total_count,record_offset))
       '''H part
        for i in range(300):
            msg2=consumer2.poll(1.0)
            if msg2 is None:
                print("Wating for message")
                continue
            elif msg2.error():
                print('error:{}'.format(msg2.error()))
            else:
                record_key2=msg2.key()
                record_value2=msg2.value()
                total_count+=1
                print("Consumer2:Consumed record with key {} and value {},\
                      and updated total count to {}"
                      .format(record_key2,record_value2,total_count))
        for i in range(300):
             msg3=consumer3.poll(1.0)
             if msg3 is None:
                 print("Wating for message:")
                 continue
             elif msg3.error():
                 print('error:{}'.format(msg3.error()))
             else:
                 record_key3=msg3.key()
                 record_value3=msg3.value()
                 total_count+=1
                 print("Consumer3:Consumed record with key {} and value {},\
                       and updated total count to {}"
                       .format(record_key3,record_value3,total_count))'''
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        #consumer2.close()
        #consumer3.close()
