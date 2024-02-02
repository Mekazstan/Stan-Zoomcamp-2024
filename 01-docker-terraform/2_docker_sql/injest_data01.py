import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main():
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')

    args = parser.parse_args()
    
    user = args.user
    password = args.password
    host = args.host
    port = args.port 
    db = args.db
    table_name = args.table_name

    csv_name = 'yellow_tripdata_2021-01.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("Connection Successfull...")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    print("Found the csv...")

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try:
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    main()
    
    
# python3 ingest_data.py --user root --password root --host e66de0f0aa8c --port 5432 --db ny_taxi --table_name yellow_taxi_data
