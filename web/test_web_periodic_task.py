import pytest
import pandas as pd
from unittest.mock import patch
from misc import run_periodic_task
from pathlib import Path

def test_run_periodic_task():
    run_periodic_task()

if __name__ == "__main__":
    pytest.main(["-v"])
