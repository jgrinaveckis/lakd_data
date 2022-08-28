from src.weather_conditions import WeatherConditions
from src.traffic_intensity import TrafficIntensity
import configparser
import argparse
import sys

if __name__ == "__main__":
    cfg = configparser.ConfigParser()
    cfg.read('configs/config.ini')
    parser = argparse.ArgumentParser(description="Provide arguments!")
    parser.add_argument("--update_specific_part",
                         type=str,
                         nargs=1, 
                         help="Select specific part to update: WeatherConditions type wc, TrafficIntensity - ti"
                        )
    args = parser.parse_args()

    #Nothing provided - go for all
    if not len(sys.argv) > 1:
        wc = WeatherConditions(db_name=cfg['SQLITE']['DbName'], 
                                url=cfg['URL']['WeatherConditions'], 
                                file_path="b", 
                                file_name="c", 
                                device_table=cfg['WeatherConditions']['DeviceTable'], 
                                measure_table=cfg['WeatherConditions']['MeasureTable'],
                                warning_table=cfg['WeatherConditions']['WarningTable']
                            )
        df_wc_measures, df_wc_devices, df_wc_warnings = wc.data_eng()
        wc.write_to_sqlite(df_wc_measures, df_wc_devices, df_wc_warnings)

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
    elif args.update_specific_part[0] == 'wc':
        wc = WeatherConditions(db_name=cfg['SQLITE']['DbName'], 
                                url=cfg['URL']['WeatherConditions'], 
                                file_path="b", 
                                file_name="c", 
                                device_table=cfg['WeatherConditions']['DeviceTable'], 
                                measure_table=cfg['WeatherConditions']['MeasureTable'],
                                warning_table=cfg['WeatherConditions']['WarningTable']
                            )
        df_wc_measures, df_wc_devices, df_wc_warnings = wc.data_eng()
        wc.write_to_sqlite(df_wc_measures, df_wc_devices, df_wc_warnings)
    elif args.update_specific_part[0] == 'ti':
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
    else:
        parser.error('Invalid argument value! See help and provide right compatible arguments!')