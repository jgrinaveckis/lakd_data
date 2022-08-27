import json
import urllib.request as req
import sqlite3
import logging
import os, sys

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

#currency no mention of different file types so using only one method for importing jsons
def import_json(url=None, file_path=None, file_name=None, dtypes=None):
    try:
        if not dtypes:
            resp = req.urlopen(url)
            if resp.getcode() != 200:
                with open(f"{file_path}\\{file_name}") as fp:
                    return json.load(fp)
            else:
                return json.loads(resp.read())
        else:
            with open(os.path.join(sys.path[0], "dtypes.json"), "r") as f:
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

