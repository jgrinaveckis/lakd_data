## LAKD (Lietuvos Automobilių Kelių Direkcija) data read

This repo is for reading two datasets provided by LAKD:
* Weather Conditions: (https://eismoinfo.lt/weather-conditions-service, https://data.gov.lt/dataset/keliu-oru-salygu-duomenys)
* Traffic Intensity: (https://eismoinfo.lt/traffic-intensity-service, https://data.gov.lt/dataset/transporto-priemoniu-eismo-duomenys)

Usage is pretty simple - run main.py with/without additional flag ```--update_specific_part```:
* Flag is not provided - both parts (WeatherCondition and TrafficIntensity are updated)
* Flag == **ti**: only TrafficIntensity are updated
* Flag == **wc** - WeatherConditions tables are updated
* Any other values for this flag leads to exit of the script.

