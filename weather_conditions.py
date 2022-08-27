import pandas as pd
import logging
from helpers import import_json, open_sqlite_connection

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class WeatherConditions:
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
        device_cols = ['id','irenginys','numeris','pavadinimas','kilometras']
        data = import_json(self.url, self.file_path, self.file_name)
        df = pd.DataFrame(data)

        #All of data imported as 'object' so further casting needed
        #As comparing is done only on 'vejo_greitis_vidut' its the one who values are casted.
        df['vejo_greitis_vidut'] = df['vejo_greitis_vidut'].astype('float64')
        df = df[(df['vejo_greitis_vidut'] >=2) & (df['matomumas'].notnull())]

        #Take device info and drop it from events table leaving only device id
        df_devices = df[device_cols]
        df_measures = df.drop(columns=device_cols[1:])
        df_measures = df_measures.loc[:,~df_measures.columns.str.startswith('konstrukcijos')]
        #df_measures['ilguma'] = df['ilguma'].astype('str')
        #https://stackoverflow.com/questions/12952546/sqlite3-interfaceerror-error-binding-parameter-1-probably-unsupported-type
        df_measures = df_measures.applymap(str)
        df_measures['surinkimo_data_unix'] = df['surinkimo_data_unix'].astype('int64')
        return df_measures, df_devices
        

    def write_to_sqlite(self, df_measures, df_devices):
        cursor, sql_conn = open_sqlite_connection(self.db_name)
        devices_tbl = cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.device_table}' ''').fetchone()
        measures_tbl = cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.measure_table}' ''').fetchone()
        #check if device table exists
        
        if devices_tbl[0] != 1:
            logging.info(f"Table {self.device_table} is not in DB")
            df_devices.to_sql(f'{self.device_table}', sql_conn, index=False)
        else:
            logging.info(f"Table {self.device_table} exists, updating...")
            current_data = cursor.execute(f""" SELECT * FROM {self.device_table}""").fetchall()
            current_ids = [line[0] for line in current_data]
            i = 0
            try:
                for index, device in df_devices.iterrows():
                    if device['id'] in current_ids:
                        cursor.execute(f""" UPDATE {self.device_table} SET irenginys=?, numeris=?, pavadinimas=?, kilometras=? WHERE id=? """,
                                        (device['irenginys'], device['numeris'], device['pavadinimas'], device['kilometras'], device['id']))
                    else:
                        cursor.execute(f""" INSERT INTO {self.device_table} VALUES(?,?,?,?,?) """,
                                        (device['id'], device['irenginys'], device['numeris'], device['pavadinimas'], device['kilometras']))
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
            logging.info(f"Table {self.measure_table} exists, updating...")
            latest_date = int(cursor.execute(f""" SELECT MAX(surinkimo_data_unix) FROM {self.measure_table} """).fetchone()[0])
            current_data = cursor.execute(f""" SELECT * FROM {self.measure_table}""").fetchall()
            #id at 3rd place
            current_ids = [line[2] for line in current_data]
            df_measures_append = df_measures[(df_measures['id'].isin(current_ids)) | (df_measures['surinkimo_data_unix'] > latest_date)]
            try:
                events_count = df_measures_append.to_sql(f"{self.measure_table}", sql_conn, if_exists='append', index=False)
                logging.info(f"Table {self.measure_table} updated successfully")
                logging.info(f"{events_count} new devices were added")
            except Exception as e:
                logging.info(e)
            
            


        



        