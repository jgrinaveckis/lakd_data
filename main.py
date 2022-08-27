from weather_conditions import WeatherConditions
import configparser

if __name__ == "__main__":
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    
    wc = WeatherConditions(db_name=cfg['SQLITE']['DbName'], 
                            url=cfg['URL']['WeatherConditions'], 
                            file_path="b", 
                            file_name="c", 
                            device_table=cfg['WeatherConditions']['DeviceTable'], 
                            measure_table=cfg['WeatherConditions']['MeasureTable']
                        )
    df_devices, df_events = wc.data_eng()

    wc.write_to_sqlite(df_devices, df_events)