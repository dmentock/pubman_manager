from pathlib import Path
from ruamel.yaml import YAML
import re
from dateutil import parser
import pandas as pd

import logging

logger = logging.getLogger(__name__)

yaml_obj = YAML(typ="unsafe")

def is_mpi_affiliation(affiliation: str) -> bool:
    return bool(re.compile(r'max[-\s–]?planck[-\s–]+i', re.IGNORECASE).search(affiliation))

def load_yaml(file_path, default_return=None):
    path = Path(file_path)
    while True:
        if not path.exists():
            logger.warning("WARNING: '%s' does not exist", path)
            return {} if default_return is None else default_return
        with path.open("r", encoding="utf-8") as fh:
            return yaml_obj.load(fh)

def save_yaml(data, file_path):
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml_obj.dump(data, fh)

def date_to_cell(date_value):
    if not date_value:
        return None
    if isinstance(date_value, str):
        return pd.to_datetime(date_value, format='%d.%m.%Y', errors='coerce', utc=True)
    elif isinstance(date_value, list) and all(isinstance(i, int) for i in date_value):
        if len(date_value) == 3:
            year, month, day = date_value
            parsed_date = parser.parse(f"{day:02d}.{month:02d}.{year}")
            return pd.to_datetime(parsed_date, format='%d.%m.%Y', errors='coerce')
        elif len(date_value) == 2:
            year, month = date_value
            parsed_date = parser.parse(f"{month:02d}.{year}")
            return pd.to_datetime(parsed_date, format='%d.%m.%Y', errors='coerce')
        elif len(date_value) == 1:
            return date_value[0]
    raise RuntimeError(f'Invalid date: "{date_value}"')