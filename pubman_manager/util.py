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

def load_yaml(file_path):
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return yaml_obj.load(fh)

def save_yaml(data, file_path):
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml_obj.dump(data, fh)

def normalize_user_id(user_id) -> str:
    user_id_str = str(user_id) if user_id is not None else ""
    return user_id_str.replace("user_", "", 1) if user_id_str.startswith("user_") else user_id_str

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
