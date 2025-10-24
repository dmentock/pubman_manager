import pytest
from pubman_manager import PROJECT_ROOT
@pytest.fixture
def test_resources_dir():
    return PROJECT_ROOT / 'test' / 'resources'