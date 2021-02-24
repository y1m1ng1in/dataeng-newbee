import json
import re
import functools
import pandas as pd
import time
from datetime import datetime

class BreadCrumbValidator:
    
    def __init__(self, df):
        self.df = df
        self.months = [
            'JAN','FEB','MAR','APR','MAY','JUN','JUL',
            'AUG','SEP','OCT','NOV','DEC'
        ]
        self.days = [31,28,31,30,31,30,31,31,30,31,30,31]
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
        self.last_committed_date = None
        
    def __generate_isnumeric_rule(self, column):
        return self.df[column].str.isnumeric()
    
    def __generate_range_rule(self, column, lo, hi, dtype):
        return [self.df[column].astype(dtype) <= hi, 
                self.df[column].astype(dtype) >= lo]
    
    def __apply_multiple(self, criterias):
        all_criterias = functools.reduce(lambda x, y: x & y, criterias)
        self.df = self.df[all_criterias]
    
    def validate(self):
        fns = [
            self.geo_direction, 
            self.event_no_trip, 
            self.vehicle_id, 
            self.geo_coordinate,
            self.act_time,
            self.velocity
        ]
        for fn in fns:
            fn()
        self.transform()
        return self.df
        
    def geo_direction(self):
        self.df = self.df[self.__generate_isnumeric_rule('DIRECTION')]
        self.__apply_multiple(self.__generate_range_rule('DIRECTION', 0, 359, int))

    def event_no_trip(self):
        self.df = self.df[self.df['EVENT_NO_TRIP'].str.len() == 9]
        
    def vehicle_id(self):
        self.df = self.df[self.df.VEHICLE_ID.isin(self.id_set)]
        
    def geo_coordinate(self):
        existence_check = [self.df[col] != '' for col 
                           in ['GPS_HDOP', 'GPS_LONGITUDE', 'GPS_LATITUDE']]
        self.__apply_multiple(existence_check)
        range_check = ([self.df.GPS_HDOP.astype(float) <= 10] 
                      + self.__generate_range_rule('GPS_LONGITUDE', -122.90, -122.00, float) 
                      + self.__generate_range_rule('GPS_LATITUDE', 45.40, 45.99, float))
        self.__apply_multiple(range_check)
    
    def act_time(self):
        self.df = self.df[self.__generate_isnumeric_rule('ACT_TIME')]
        self.__apply_multiple(self.__generate_range_rule('ACT_TIME', 15899, 90601, int))

    def velocity(self):
        self.df = self.df[self.__generate_isnumeric_rule('VELOCITY')]
        self.__apply_multiple(self.__generate_range_rule('VELOCITY', 0, 40, int))
    
    def transform(self):
        self.df = self.df.astype({
            'ACT_TIME': 'int32',
            'VELOCITY': 'int32',
        })
        self.df['VELOCITY'] = 0.000621 * self.df['VELOCITY'] * 3600
        make_ts = lambda opd_date: time.mktime(
            datetime.strptime(opd_date, '%d-%b-%y').timetuple()) 
        self.df['TS'] = self.df['OPD_DATE'].apply(make_ts) + self.df['ACT_TIME']


class EventStopValidator:

    def __init__(self, df):
        self.df = df

    def validate(self):
        fns = [
            self.drop_null, 
            self.field_consistency, 
            self.drop_duplicates
        ]
        for fn in fns:
            fn()
        return self.df

    def drop_null(self):
        """ Drop all rows with null field
        """
        self.df = self.df.loc[~self.df['direction'].isnull()]
    
    def field_consistency(self):
        """ Check if for each trip_id, there is only one combination of 
            vehicle_number, route_number, direction, and service_key.
            If this assertion is violated, then all rows associated with
            that trip_id will be discarded. 
        """
        grouped = self.df.groupby('trip_id').agg(['nunique'])
        grouped = grouped[grouped.apply(pd.Series.nunique, axis=1) == 1]
        self.df = self.df[self.df['trip_id'].isin(grouped.index.tolist())]

    def drop_duplicates(self):
        """ Drop duplicated rows in a df
        """
        self.df = self.df.drop_duplicates()


class IntegratedValidator:
    """ Validate integrated entries of breadcrumb data and 
        event stop data. 
    """
    
    def __init__(self, df_breadcrumbs, df_event_stops):
        self.df_breadcrumbs = df_breadcrumbs
        self.df_event_stops = df_event_stops
        self.__join()
        
    def __join(self):
        """ Join breadcrumb dataframe with event stop data frame
            on trip id and vehicle number.
        """
        self.df = pd.merge(
            self.df_breadcrumbs, 
            self.df_event_stops,  
            how='left', 
            left_on=['EVENT_NO_TRIP','VEHICLE_ID'], 
            right_on=['trip_id', 'vehicle_number'])
        
    def validate(self):
        self.schedule_deviation_existance()
        return self.df

    def schedule_deviation_existance(self):
        """ Drop all rows such that it has a route number or direction 
            but it doesn't have a schedule deviation
        """
        self.df = self.df.loc[
            ~((self.df['SCHEDULE_DEVIATION'] == '') 
               & (~(self.df['route_number'].isnull() 
                    | self.df['direction'].isnull())))]
        