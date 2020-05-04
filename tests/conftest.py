import os
from pathlib import Path
from typing import NamedTuple

import pytest
from optimal_solar_with_storage.nrel_service import UserInfo, Location


class FakeNREL:
    def __init__(self, data):
        self.data = data

    def query_for_data(self, *args, **kwargs):
        return self.data


@pytest.fixture
def test_dir():
    directory = Path(__file__).parent
    return directory


@pytest.fixture
def nrel_user_info():
    return UserInfo(
        os.getenv("NREL_API_KEY"),
        os.getenv("NREL_ACCOUNT_EMAIL"),
        os.getenv("NREL_ACCOUNT_USER"),
    )


@pytest.fixture
def amarillo():
    return Location(35.2147102, -101.9268218)


@pytest.fixture
def fake_nrel(test_dir):
    test_file = test_dir / "test_data" / "test_data_2018.txt"
    with test_file.open() as f:
        data = f.read()

    return FakeNREL(data)
