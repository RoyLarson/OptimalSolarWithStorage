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
from io import StringIO
from typing import List, Tuple, Iterable, NamedTuple

from requests.packages.urllib3 import Retry
import requests
import pandas as pd

ATTRIBUTES = {
    "air_temperature",
    "clearsky_dhi",
    "clearsky_dni",
    "clearsky_ghi",
    "cloud_type",
    "dew_point",
    "dhi",
    "dni",
    "fill_flag",
    "ghi",
    "relative_humidity",
    "solar_zenith_angle",
    "surface_albedo",
    "surface_pressure",
    "total_precipitable_water",
    "wind_direction",
    "wind_speed",
}


class UserInfo(NamedTuple):
    api_key: str
    email: str
    user_name: str


class Location(NamedTuple):
    latitude: float
    longitude: float


class NREL_Data_Source:
    def __init__(
        self,
        nrel_user: UserInfo,
        api_root="https://developer.nrel.gov/api/solar/nsrdb_psm3_download.csv?",
    ):
        self.user = nrel_user
        self.api_root = api_root

    def query_for_data(
        self, location: Location, year: int, attributes: Iterable[str]
    ) -> str:
        """Query NREL for data

        Args:
            location: a Location with the latitude and longitude
            year: year to get data for
            attributes: list of attributes to get

        Returns:
            str: 1st 2 lines are metadata about the measurements 
                3rd line is the header of row of the rest of the values
                4th line on is the data
        """
        request_url = self.create_url(location, year, attributes)
        http = requests.adapters.HTTPAdapter(max_retries=0)
        https = requests.adapters.HTTPAdapter(max_retries=0)
        with requests.Session() as session:
            session.mount("http://", http)
            session.mount("https://", https)
            with session.get(request_url) as resp:
                resp.raise_for_status()
                data = resp.text
        return data

    def create_url(
        self, location: Location, year: int, attributes: Iterable[str]
    ) -> str:
        if isinstance(attributes, str):
            attributes = [attributes]

        user_text = f"api_key={self.user.api_key}&email={self.user.email}&full_name={self.user.user_name.replace(' ', '%20')}"
        wkt_text = f"wkt=POINT({location.longitude}+{location.latitude})"
        year_and_interval_text = f"names={year}&interval=30"
        attribute_text = f"attributes={','.join(attributes)}"
        return self.api_root + "&".join(
            [user_text, wkt_text, year_and_interval_text, attribute_text]
        )


def convert_response_to_dfs(data: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return DataFrames of data from NREL Data

    Args:
        data: The response text from NREL Data

    Returns:
        tuple (metadata, data)
        metadata = pd.DataFrame([measurement, measurement metadata], columns = ['measurement', 'measurement meta])
        data = pd.DataFrame(measurements, columns=attributes, index=pd.DateTimeIndex(every 15 minutes for year))
    """
    lines = data.split("\n")
    metadata = pd.DataFrame(
        [
            (measurement.strip(), metadata.strip())
            for measurement, metadata in zip(lines[0].split(","), lines[1].split(","))
        ],
        columns=["measurement", "metadata"],
    )
    f = StringIO("\n".join(lines[2:]))

    data = pd.read_csv(f)
    data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_')
    data["datetime"] = pd.to_datetime(data[["year", "month", "day", "hour", "minute"]])
    data = data.set_index("datetime", drop=True)
    data = data.drop(columns=["year", "month", "day", "hour", "minute"])
    rename_cols = {}
    if 'temperature' in data.columns:
        rename_cols['temperature']='air_temperature'
    if 'pressure' in data.columns:
        rename_cols['pressure'] = 'surface_pressure'
    if 'precipitable_water' in data.columns:
        rename_cols['precipitable_water'] = 'total_precipitable_water'
    if rename_cols:
        data = data.rename(rename_cols, axis=1)

    return metadata, data


def get_solar_data_for_location(
    data_source: NREL_Data_Source,
    location: Location,
    year: int,
    attributes: Iterable[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    if not set(attributes) <= ATTRIBUTES:
        raise AssertionError(f"{attributes-ATTRIBUTES} not valid attributes")

    data = data_source.query_for_data(location, year, attributes)
    return convert_response_to_dfs(data)
