from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from geopy.distance import geodesic
import pandas as pd
import json
from sqlalchemy import (MetaData, Table, Column, Integer, 
                        Text, DateTime, BigInteger, Float)


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

def _calculate_distance(row):
    '''calculate distance of two point
       first shift location col 1 step for getting prev location
       then load location json and calculate distance
       finally will apply to dataframe'''
    try:
        location = json.loads(row['location'])
        prev_location = json.loads(row['prev_location'])
        point = (location['latitude'], location['longitude'])
        point_prev = (prev_location['latitude'], prev_location['longitude'])
        desic = geodesic(point_prev, point).km
    except TypeError as e:
        desic = 0
    return desic


def connect_to_mysql():
    '''connecting to mysql database
       with createing engine with sqlalchemy'''
    while True:
        try:
            mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
            break
        except OperationalError:
            sleep(0.1)
    print('Connection to MySql successful.')
    return mysql_engine


def fetch_all_devices(psql):
    '''it receives all the data from the device table 
       and sorts it based on the device_id and time column 
       to make distance calculations easier'''
    while True:
        try:
            query = "SELECT * FROM devices ORDER BY device_id, time"
            df = pd.read_sql(query, psql)
            print('Data fetched successfully.')
            break
        except Exception as e:
            print(f'Error fetching data: {str(e)}')
            sleep(0.1)
    return df


def aggreagte_device_per_hour(df):
    '''it aggregates data from the device table 
       based on the device_id and hour of time column 
       then applies agg functions for calculate
       max_temperature, data_points, total_distance'''
       
    aggregated_df = df.groupby(['device_id', pd.Grouper(key='time', freq='H')]).agg(
                        max_temperature=('temperature', 'max'),
                        data_points=('temperature', 'count'),
                        total_distance=('distance', 'sum')
                    ).reset_index()
    return aggregated_df


def create_schema(engine, table):
    '''creates the required table schema
       for the aggregated data'''
       
    metadata = MetaData()
    agg_devices = Table(
        table,
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('device_id', Text),
        Column('time', DateTime),
        Column('max_temperature', BigInteger),
        Column('data_points', BigInteger),
        Column('total_distance', Float)
    )
    metadata.create_all(engine)


def load_dataframe_to_sqldb(df, table, connection):
    '''loads pandas dataframe to sql database'''
    while True:
        try:
            df.to_sql(table, connection, if_exists='append', index=False) 
            break
        except OperationalError:
            sleep(0.1)
    print('dataframe loaded into database')


# extract section
df = fetch_all_devices(psql_engine)

# transform section
df['prev_location'] = df.groupby('device_id')['location'].shift(1)
df['distance'] = df.apply(_calculate_distance, axis=1)
df = df.drop(columns=['prev_location'])
df['time'] = pd.to_datetime(df['time'], unit='s')
df = aggreagte_device_per_hour(df)

# load section
_table = "agg_devices"
mysql_engine = connect_to_mysql()
create_schema(mysql_engine, _table)
load_dataframe_to_sqldb(df, _table, mysql_engine)

# we can use airflow to schedule this etl task
# and run the task daily and get the previous day's data
