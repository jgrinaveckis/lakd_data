import json
import urllib.request as req
import sqlite3
import logging
import os, sys
import pandas as pd

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

#currency no mention of different file types so using only one method for importing jsons
def import_json(url=None, file_path=None, file_name=None, dtypes=None):
    try:
        if not dtypes:
            resp = req.urlopen(url)
            if resp.getcode() != 200:
                with open(os.path.join(file_path, file_name)) as fp:
                    return json.load(fp)
            else:
                return json.loads(resp.read())
        else:
            with open(os.path.join(sys.path[0], "configs/dtypes.json"), "r") as f:
                return json.load(f)
    except Exception as e:
        print(e)

def open_sqlite_connection(db_name):
    try:
        sql_conn = sqlite3.connect(db_name)
        cursor = sql_conn.cursor()
        logging.info(f"Successfully connected to DB: {db_name}")
        return cursor, sql_conn
    except sqlite3.Error as err:
        logging.info(err)

def check_table_availability(cursor, table_name):
    """
        Returns count of tables based on provided table_name
    """
    tbl = cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}' ''').fetchone()
    return tbl

def update_table(sql_conn, cursor, df:pd.DataFrame, table_name, id_column, date_column, unix_date=True):
    logging.info(f"Table {table_name} exists, updating...")
    if unix_date:
        latest_date = int(cursor.execute(f""" SELECT MAX({date_column}) FROM {table_name} """).fetchone()[0])
    else:
        latest_date = cursor.execute(f""" SELECT MAX({date_column}) FROM {table_name} """).fetchone()[0]
    current_data = cursor.execute(f""" SELECT DISTINCT {id_column} FROM {table_name}""").fetchall()
            
    current_ids = [line[0] for line in current_data]
    df_measures_append = df[(~df[id_column].isin(current_ids)) | (df[date_column] > latest_date)]
    try:
        events_count = df_measures_append.to_sql(f"{table_name}", sql_conn, if_exists='append', index=False)
        logging.info(f"Table {table_name} updated successfully")
        logging.info(f"{events_count} new devices were added")
    except Exception as e:
        logging.info(e) 

