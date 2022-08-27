import pandas as pd
import logging
from helpers import import_json, open_sqlite_connection

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class TrafficIntensity:
    def __init__(self,
                 db_name:str,
                 url:str, 
                 file_path:str, 
                 file_name:str,
                 device_table:str,
                 measure_table:str):
        self.db_name = db_name
        self.url = url
        self.file_path = file_path
        self.file_name = file_name
        self.device_table = device_table
        self.measure_table = measure_table

    def data_eng(self):
        data = import_json(self.url, self.file_path, self.file_name)
        df = pd.DataFrame(data)


        df_measures = df[['id', "roadSegments", 'timeInterval', 'date']]
        df_devices = df.drop(columns=['roadSegments', 'timeInterval', 'date'])

        #make multiple lines for negative and positive directions
        df_measures = df_measures.explode('roadSegments').reset_index().drop(columns='index')
        # concat current columns and parsed ones. Not sure about start-end X/Y, direction, winter/summer speeds - if it belongs to device.
        # currently leaving to measures.
        df_measures = pd.concat([df_measures.drop(['roadSegments'], axis=1), df_measures['roadSegments'].apply(pd.Series)], axis=1)
        df_measures.drop(columns=[0], inplace=True)
        #TODO: all dtypes are object from json. cast to proper ones based on schema. See not compatible data types between python and sqlite3
        df_measures['averageSpeed'] = df_measures['averageSpeed'].astype('float64')
        df_measures = df_measures[df_measures['averageSpeed'] > 80]


        return df_devices, df_measures

    def write_to_sqlite(self, df_measures, df_devices):
        cursor, sql_conn = open_sqlite_connection(self.db_name)
        devices_tbl = cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.device_table}' ''').fetchone()
        measures_tbl = cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.measure_table}' ''').fetchone()

        if devices_tbl[0] != 1:
            logging.info(f"Table {self.device_table} is not in DB")
            added_rows = df_devices.to_sql(f'{self.device_table}', sql_conn, index=False)
            logging.info(f"Added {added_rows} rows")

        