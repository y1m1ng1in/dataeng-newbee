#!/usr/bin/env python3
import psycopg2
import psycopg2.extras
import logging
from constants import DBname, DBuser, DBpwd, TableName


def formsql(data: list):
    bc = ''
    trip = ''
    for d in data:
        bc += f"""
        INSERT INTO {TableName[0]} VALUES (
        to_timestamp({d['TS']}), -- time
        {d['GPS_LATITUDE']},
        {d['GPS_LONGITUDE']},
        {d['DIRECTION']}, -- direction
        {d['VELOCITY']}, -- speed
        {d['EVENT_NO_TRIP']} -- trip id
        );
        """
        trip += f"""
        INSERT INTO {TableName[1]} VALUES (
        {d['EVENT_NO_TRIP']}, -- trip id
        NULL,
        {d['VEHICLE_ID']},
        NULL,
        NULL
        )
        ON CONFLICT DO NOTHING;
        """

    return trip + bc


def insert(conn, cmd):
    with conn.cursor() as cursor:
        cursor.execute(cmd)

    logging.info('all data inserted.')


def insert_batch(conn, records):
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor,f"""
        INSERT INTO {TableName[1]} VALUES (
        %(EVENT_NO_TRIP)s, -- trip id
        %(route_number),
        %(VEHICLE_ID)s,
        %(service_key),
        %(direction)
        )
        ON CONFLICT DO NOTHING;
        """, records)

        psycopg2.extras.execute_batch(cursor,f"""
        INSERT INTO {TableName[0]} VALUES (
        to_timestamp(%(TS)s), -- time
        %(GPS_LATITUDE)s,
        %(GPS_LONGITUDE)s,
        %(DIRECTION)s, -- direction
        %(VELOCITY)s, -- speed
        %(EVENT_NO_TRIP)s -- trip id
        );
        """, records)


def connect():
    connection = psycopg2.connect(
        host = 'localhost',
        database = DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection


def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
        drop table if exists BreadCrumb;
        drop table if exists Trip;
        drop type if exists service_type;
        drop type if exists tripdir_type;

        create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
        create type tripdir_type as enum ('Out', 'Back');

        create table Trip (
            trip_id integer,
            route_id integer,
            vehicle_id integer,
            service_key service_type,
            direction tripdir_type,
            PRIMARY KEY (trip_id)
        );

        create table BreadCrumb (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer,
            FOREIGN KEY (trip_id) REFERENCES Trip
        );
        """)

    logging.info('Two table created.')


def data_insert(record):
    conn = connect()

    insert_batch(conn, records=record)


if __name__ == '__main__':
    createTable(connect())
