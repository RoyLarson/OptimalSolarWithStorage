import os
from pathlib import Path

from dotenv import load_dotenv
from optimal_solar_with_storage.nrel_service import CachedNRELData, NREL_Data_Source, get_solar_data_for_location, Location, ATTRIBUTES
import pandas as pd

load_dotenv()
DEBUG = eval(os.getenv("DEBUG"))

if DEBUG:
    user_profile = Path(os.getenv('USERPROFILE'))
    data_folder = user_profile/os.getenv('TEST_DATA')
    data_source = CachedNRELData(data_folder)

solar_data = []
for i in range(1999,2019):
    meta_data, year_data = get_solar_data_for_location(data_source, Location(0,0), i, ATTRIBUTES)
    solar_data.append(year_data)
solar_data=pd.concat(solar_data)
print(solar_data.head())
print(solar_data.tail())

