import os
from typing import NamedTuple

import pytest
from optimal_solar_with_storage.nrel_service import UserInfo, Location


@pytest.fixture
def nrel_user_info():
    return UserInfo(os.getenv("NREL_API_KEY"), os.getenv("NREL_ACCOUNT_EMAIL"), os.getenv("NREL_ACCOUNT_USER"))

@pytest.fixture
def amarillo():
    return Location(35.2147102,-101.9268218)
    