from pathlib import Path

import pytest

from sodaspark.scan import Scan


@pytest.fixture
def scan_data_frame_path() -> Path:
    return Path(__file__).parent.absolute() / "data/scan_data_frame.yml"


@pytest.fixture
def scan(scan_data_frame_path: Path) -> Scan:
    return Scan(scan_data_frame_path)
