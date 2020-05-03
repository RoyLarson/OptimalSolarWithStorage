import pytest
from requests.packages.urllib3 import Retry
import requests

from optimal_solar_with_storage import nrel_service

@pytest.mark.skip("Queries Real Source")
def test_nrel_api(nrel_user_info, amarillo):
    url = (
            "https://developer.nrel.gov/api/solar/nsrdb_psm3_download.csv?"
            f"api_key={nrel_user_info.api_key}&full_name={nrel_user_info.user_name}&email={nrel_user_info.email}"
            f"&wkt=POINT({amarillo.longitude}+{amarillo.latitude})&interval=30&names=2010&"
            "attributes=air_temperature,clearsky_dhi,clearsky_dni,clearsky_ghi,cloud_type,dew_point,"
            "dhi,dni,fill_flag,ghi,relative_humidity,solar_zenith_angle,surface_albedo,surface_pressure,"
            "total_precipitable_water,wind_direction,wind_speed"
    )
    http = requests.adapters.HTTPAdapter(max_retries=0)
    https = requests.adapters.HTTPAdapter(max_retries=0)
    with requests.Session() as session:
        session.mount('http://', http)
        session.mount('https://', https)
        with session.get(url) as resp:
            resp.raise_for_status()
            data = resp.text

    assert data

def test_create_url(nrel_user_info, amarillo):
    data_source = nrel_service.NREL_Data_Source(nrel_user_info)
    query_str = data_source.create_url(amarillo, 2018, ['wind_direction'])
    answer = (
            "https://developer.nrel.gov/api/solar/nsrdb_psm3_download.csv?"
            f"api_key={nrel_user_info.api_key}&email={nrel_user_info.email}&full_name={nrel_user_info.user_name}"
            f"&wkt=POINT({amarillo.longitude}+{amarillo.latitude})&names=2018&interval=30"
            "&attributes=wind_direction"
    )
    assert query_str == answer


