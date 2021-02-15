#!/usr/bin/env python

import json
import re
from datetime import datetime
import time

class Validator:

    def __init__(self, logger):
        self.months = [
            'JAN','FEB','MAR','APR','MAY','JUN','JUL',
            'AUG','SEP','OCT','NOV','DEC'
        ]
        self.days = [31,28,31,30,31,30,31,31,30,31,30,31]
        self.most_months = [0 for _ in range(13)]
        self.most_days   = [0 for _ in range(33)]
        self.id_set = [
            '1776','2218','2220','2223','2224','2226','2227','2228','2231','2232',
            '2233','2235','2237','2238','2239','2240','2241','2242','2243','2244',
            '2245','2246','2247','2248','2249','2250','2251','2262','2263','2264',
            '2265','2266','2267','2268','2269','2270','2271','2272','2273','2274',
            '2275','2276','2277','2278','2279','2280','2281','2282','2283','2284',
            '2285','2286','2287','2288','2289','2290','2291','2292','2293','2294',
            '2401','2402','2403','2404','2901','2902','4001','4002','4003','4004',
            '4005','4006','4007','4008','4009','4010','4011','4012','4013','4014',
            '4015','4016','4017','4018','4019','4020','4021','4022','4023','4024',
            '4025','4026','4027','4028','4029','4030','4031','4032','4033','4034',
            '4035','4036','4037','4038','6001','6002','6003','6004','6005','6006',
            '6007','6008','6009','6010','6011','6012'
        ]
        self.keys = set()
        self.logger = logger
        self.last_committed_date = None

    def validate(self, j):
        fns = [
            self.geo_coordinate,
            self.event_no_trip,
            self.vehicle_id,
            self.geo_coordinate,
            #self.primary_key_uniqueness,
            self.act_time,
            self.opd_date,
            self.geo_direction,
            self.velocity
        ]
        for fn in fns:
            j, should_save = fn(j)
            if not should_save:
                return j, False
        j['TS'] = time.mktime(datetime.strptime(j['OPD_DATE'], '%d-%b-%y').timetuple()) + float(j['ACT_TIME'])
        return j, True

    def geo_direction(self, j):
        if len(j['DIRECTION']) == 0:
            print("Missing Direction!")
            return j, False
        direction = int(j['DIRECTION'])
        if direction not in range(0, 360):
            print("Direction error! Direction is ", direction)
            return j, False
        return j, True

    def event_no_trip(self, j):
        if len(j['EVENT_NO_TRIP']) == 0:
            print("Missing Event no trip!")
            return j, False
        return j, True

    def vehicle_id(self, j):
        if len(j['VEHICLE_ID']) == 0 or len(j['VEHICLE_ID']) != 4:
            print("Missing Vehicle id!")
            return j, False
        vehicle_id = j['VEHICLE_ID']
        if(vehicle_id not in self.id_set):
            print("Vehicle id is not in table! Vehicle id is ", vehicle_id)
            return j, False
        return j, True

    def geo_coordinate(self, j):
        if len(j['GPS_HDOP']) == 0: # GPH_HDOP must not be missing
            return j, False

        gps_hdop = j['GPS_HDOP']
        if float(gps_hdop) > 10:    # Discard the row if it lacks of precision
            return j, False

        gps_longitude = j['GPS_LONGITUDE']
        gps_latitude  = j['GPS_LATITUDE']

        if len(j['GPS_LONGITUDE']) == 0 or len(j['GPS_LATITUDE']) == 0:
            # If it does not lack of precision, then coordinate must not be missing
            print("hdop < 10, GPS_LONGITUDE or GPS_LATITUDE missing")
            return j, False

        longitude = float(gps_longitude)
        latitude  = float(gps_latitude)
        if (
            longitude >- 122.00 or longitude <- 122.90
            or latitude > 45.99 or latitude < 45.40
        ):
            print("Longitude Latitude error!", longitude, latitude)
            return j, False

        return j, True

    def primary_key_uniqueness(self, j):
        if (
            len(j['OPD_DATE']) == 0
            or len(j['ACT_TIME']) == 0
            or len(j['VEHICLE_ID']) == 0
        ):
            print("Missing Joint primary key!")
            return j, False

        joint_key = j['OPD_DATE'] + j['ACT_TIME'] + j['VEHICLE_ID']
        if joint_key in self.keys:
            print("Joint key already in set! ", joint_key)
            return j, False

        self.keys.add(joint_key)
        return j, True


    def act_time(self, j):
        if len(j['ACT_TIME']) == 0:
            print("Missing Act time!")
            return j, False
        act_time = j['ACT_TIME']
        time = int(act_time)
        if time not in range(15900, 90600):
            print("Act Time error! Time is ", time)
            return j, False
        return j, True

    def velocity(self, j):
        if len(j['VELOCITY']) == 0:
            print("Discard the row without a velocity")
            return j, False
        speed = int(j['VELOCITY'])
        if speed > 40:
            print("Speed exceed the upper bound of our assertion")
            return j, False
        j['VELOCITY'] = str(0.000621 * speed * 3600)
        return j, True

    def opd_date(self, j):
        pattern = r'[-]'
        opd_date = j['OPD_DATE']
        if len(opd_date) == 0:
            print("Missing OPD_Date")
            return self.__interpolate_opd_date(j)

        day_mon_year = re.split(pattern, opd_date)
        day  = day_mon_year[0]
        mon  = day_mon_year[1]
        year = day_mon_year[2]
        if (
            year == '20'
            and mon in self.months
            and int(day) < self.days[self.months.index(mon)]
        ):
            self.last_committed_date = (day, mon, year)
            return j, True
        return self.__interpolate_opd_date(j)

    def __interpolate_opd_date(self, j):
        if self.last_committed_date == None:
            return j, False
        interpolated_day  = self.last_committed_date[0]
        interpolated_mon  = self.last_committed_date[1]
        interpolated_year = self.last_committed_date[2]
        j['OPD_DATE'] = "{}-{}-{}".format(
            interpolated_day,
            interpolated_mon,
            interpolated_year)
        return j, True
