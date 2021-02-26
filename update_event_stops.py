#!/usr/bin/env python3
'''
This program manually updates the missing field in Trip table 
given event stop data. 
'''

import psycopg2
import psycopg2.extras
import csv
import re
import logging
import time
import argparse
import pandas as pd
from datetime import datetime
from constants import DBname, DBuser, DBpwd, TableName
from helper import load_logger
from new_validation import EventStopValidator

# test
from validation import Validator
import json


def formsql(data: list):
    trip = ''
    for d in data:
        trip += f"""
        UPDATE {TableName[1]} SET
        route_id = {d['route_number']},
        service_key = {d['service_key']},
        direction = {d['direction']}
        WHERE trip_id = {d['trip_id']}
        ON CONFLICT DO NOTHING;
        """
    return trip

def connect():
    connection = psycopg2.connect(
        host = 'localhost',
        database = DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

def data_update():
    parser = argparse.ArgumentParser(description='Update null field in trip')
    parser.add_argument('--data', type=str, help='json file')
    args = parser.parse_args()
    
    with open(args.data) as f:
        data = json.load(f)
        print(data)
        f.close()
    df = pd.DataFrame.from_dict(data)
    validator   = EventStopValidator(df)
    valid_dicts = validator.validate().to_dict('records')

    sql  = formsql(valid_dicts)
    conn = connect()
    with conn.cursor() as cursor:
        cursor.execute(sql)

if __name__ == '__main__':
    data_update()
