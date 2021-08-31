from pathlib import Path

import pytest


@pytest.fixture
def scan_data_frame_path() -> Path:
    return Path(__file__).parent.absolute() / "data/scan_data_frame.yml"
