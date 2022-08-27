from weather_conditions import WeatherConditions
from traffic_intensity import TrafficIntensity
import configparser

if __name__ == "__main__":
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    
    # wc = WeatherConditions(db_name=cfg['SQLITE']['DbName'], 
    #                         url=cfg['URL']['WeatherConditions'], 
    #                         file_path="b", 
    #                         file_name="c", 
    #                         device_table=cfg['WeatherConditions']['DeviceTable'], 
    #                         measure_table=cfg['WeatherConditions']['MeasureTable']
    #                     )
    # df_devices, df_measures = wc.data_eng()
    # wc.write_to_sqlite(df_measures, df_devices)


    ti = TrafficIntensity(
                            db_name=cfg['SQLITE']['DbName'], 
                            url=cfg['URL']['TrafficIntensity'], 
                            file_path="b", 
                            file_name="c", 
                            device_table=cfg['TrafficIntensity']['DeviceTable'], 
                            measure_table=cfg['TrafficIntensity']['MeasureTable']
                        )
    df_ti_devices, df_ti_measures = ti.data_eng()

    ti.write_to_sqlite(df_ti_measures, df_ti_devices)