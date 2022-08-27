import json
import urllib.request as req
import sqlite3
import logging

FILE_PATH = "C:\\Users\\grina\\Desktop"
FILE_NAME = "test.json"
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

#currency no mention of different file types so using only one method for importing jsons
def import_json(url:str, file_path, file_name):
    resp = req.urlopen(url)
    if resp.getcode() != 200:
        with open(f"{file_path}\\{file_name}") as fp:
            try:
                return json.load(fp)
            except Exception as e:
                print(e)
    else:
        try: 
            return json.loads(resp.read())
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
