from pathlib import Path
from ruamel.yaml import YAML
import re

import logging

logger = logging.getLogger(__name__)

yaml_obj = YAML(typ="unsafe")

def is_mpi_affiliation(affiliation: str) -> bool:
    return bool(re.compile(r'max[-\s–]?planck', re.IGNORECASE).search(affiliation))

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