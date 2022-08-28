from datetime import datetime
import pandas as pd
import logging
from helpers import import_json, open_sqlite_connection, check_table_availability, update_table

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class TrafficIntensity:
    def __init__(self,
                 db_name:str,
                 url:str, 
                 device_table:str,
                 measure_table:str,
                 file_path=None, 
                 file_name=None):
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
        df_measures.rename(columns={'id':'device_id'}, inplace=True)
        df_devices = df.drop(columns=['roadSegments', 'timeInterval', 'date'])
        #automatic conversion by pandas is spot on.
        df_measures = df_measures.convert_dtypes()
        df_devices = df_devices.convert_dtypes()

        #make multiple lines for negative and positive directions
        df_measures = df_measures.explode('roadSegments').reset_index().drop(columns='index')
        # concat current columns and parsed ones. Not sure about start-end X/Y, direction, winter/summer speeds - if it belongs to device.
        # currently leaving to measures.
        df_measures = pd.concat([df_measures.drop(['roadSegments'], axis=1), df_measures['roadSegments'].apply(pd.Series)], axis=1)
        df_measures.drop(columns=[0], inplace=True)

        df_measures['date'] = pd.to_datetime(df_measures['date'], format="%Y-%m-%dT%H:%M:%S.%f%z")
        df_measures = df_measures[df_measures['averageSpeed'] > 80]
        return df_devices, df_measures

    def write_to_sqlite(self, df_measures, df_devices):
        cursor, sql_conn = open_sqlite_connection(self.db_name)
        devices_tbl = check_table_availability(cursor, self.device_table)
        measures_tbl = check_table_availability(cursor, self.measure_table)

        if devices_tbl[0] != 1:
            logging.info(f"Table {self.device_table} is not in DB, creating...")
            added_rows = df_devices.to_sql(f'{self.device_table}', sql_conn, index=False)
            logging.info(f"Added {added_rows} devices")
        else:
            logging.info(f"Table {self.device_table} exists, updating...")
            current_data = cursor.execute(f""" SELECT * FROM {self.device_table}""").fetchall()
            current_ids = [line[0] for line in current_data]
            i = 0
            try:
                for index, device in df_devices.iterrows():
                    if device['id'] in current_ids:
                        cursor.execute(f""" UPDATE {self.device_table} SET name=?, roadNr=?, roadName=?, km=?, x=?, y=? WHERE id=? """,
                                        (device['name'], device['roadNr'], device['roadName'], device['km'], device['x'], device['y'], device['id']))
                    else:
                        cursor.execute(f""" INSERT INTO {self.device_table} VALUES(?,?,?,?,?,?,?) """,
                                        (device['id'], device['name'], device['roadNr'], device['roadName'], device['km'], device['x'], device['y']))
                        i+=1
                logging.info(f"Table {self.device_table} updated successfully")
                logging.info(f"{i} new devices were added")
            except Exception as e:
                logging.info(e)

        if measures_tbl[0] != 1:
            logging.info(f"Table {self.measure_table} is not in DB")
            try:
                appended_rows = df_measures.to_sql(f'{self.measure_table}', sql_conn, index=False)
                #current_data = cursor.execute(f""" SELECT * FROM {self.measure_table}""").fetchall()
                logging.info(f"Successfully added {appended_rows} rows")
            except Exception as e:
                logging.info(e)
        else:
            update_table(sql_conn, cursor, df_measures, self.measure_table, 'device_id', 'date', unix_date=False)

        