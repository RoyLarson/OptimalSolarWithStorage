"""
Get Solar Data From https://developer.nrel.gov/

Available Attributes:
    air_temperature,
    clearsky_dhi,
    clearsky_dni,
    clearsky_ghi,
    cloud_type,
    dew_point,
    dhi,
    dni,
    fill_flag,
    ghi,
    relative_humidity,
    solar_zenith_angle,
    surface_albedo,
    surface_pressure,
    total_precipitable_water,
    wind_direction,
    wind_speed

Years Availabe 1998 - 2018

Request Parameters:
    Parameter 	    Required 	Value                               Default  
    api_key 	    Yes 	Type: string                            Default: None 
    wkt 	        Yes 	Type: well-known text string            Default: None
    attributes 	    No 	    Type: comma delimited string array      Default: Returns ALL
    names 	        Yes 	Type: comma delimited string array      Default: None 
    utc 	        No 	    Type: true or false                     Default: true
    leap_day 	    No 	    Type: true or false                     Default: false
    interval 	    Yes     Type: 30 or 60                          Default: None
    full_name 	    No 	    Type: string                            Default: None
    email 	        Yes     Type: email string                      Default: None
    affiliation     No 	    Type: string                            Default: None
    reason 	        No 	    Type: string                            Default: None
    mailing_list 	No 	    Type: true or false                     Default: false
"""
from dataclasses import dataclass
from typing import List, Tuple, Iterable, NamedTuple

from requests.packages.urllib3 import Retry
import requests
import pandas as pd

ATTRIBUTES = {'air_temperature',     'clearsky_dhi',
    'clearsky_dni',
    'clearsky_ghi',
    'cloud_type',
    'dew_point',
    'dhi',
    'dni',
    'fill_flag',
    'ghi',
    'relative_humidity',
    'solar_zenith_angle',
    'surface_albedo',
    'surface_pressure',
    'total_precipitable_water',
    'wind_direction',
    'wind_speed'
}
class UserInfo(NamedTuple):
    api_key:str
    email:str
    user_name:str

class Location(NamedTuple):
    latitude: float
    longitude: float

class NREL_Data_Source:
    def __init__(self, nrel_user:UserInfo, api_root='https://developer.nrel.gov/api/solar/nsrdb_psm3_download.csv?'):
        self.user=nrel_user
        self.api_root=api_root

    def query_for_data(self, location, year, attributes):
        request_url = self.create_url(location, year, attributes)
        http = requests.adapters.HTTPAdapter(max_retries=0)
        https = requests.adapters.HTTPAdapter(max_retries=0)
        with requests.Session() as session:
            session.mount('http://', http)
            session.mount('https://', https)
            with session.get(request_url) as resp:
                resp.raise_for_status()
                data = resp.text
        return data

    def create_url(self, location:Location, year:int, attributes:Iterable[str])->str:
        if isinstance(attributes, str):
            attributes = [attributes]

        user_text=f"api_key={self.user.api_key}&email={self.user.email}&full_name={self.user.user_name.replace(' ', '%20')}"
        wkt_text = f"wkt=POINT({location.longitude}+{location.latitude})"
        year_and_interval_text = f"names={year}&interval=30"
        attribute_text = f"attributes={','.join(attributes)}"
        return self.api_root+'&'.join([user_text, wkt_text, year_and_interval_text, attribute_text])


def get_solar_data_for_location(data_source: NREL_Data_Source, location:Location, year:int, attributes:Iterable[str])->Tuple[pd.DataFrame, pd.DataFrame]:
    """Get data from an NREL data source.

    Args:
        data_source: A data source to get the query
        location: the latitude and longitude of the location
        year:  the year to get the day
        attributes:  See above for available attributes to request

    Returns:
        DataFrame containing metadata about the measurements
        DataFrame containing requested measurements
    """
    if not set(attributes)<=ATTRIBUTES:
        raise AssertionError(f"{attributes-ATTRIBUTES} not valid attributes")

    data = data_source.query_for_data(location, year, attributes)




