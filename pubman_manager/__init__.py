from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent

PUBMAN_CACHE_DIR = PROJECT_ROOT / 'pubman_cache'
PUBMAN_CACHE_DIR.mkdir(exist_ok=True)

FILES_DIR = PROJECT_ROOT / 'files'
FILES_DIR.mkdir(exist_ok=True)

PUBLICATIONS_DOIS_DIR = PROJECT_ROOT / 'publications_dois'
PUBLICATIONS_DOIS_DIR.mkdir(exist_ok=True)

PUBLICATIONS_DIR = PROJECT_ROOT / 'publications'
PUBLICATIONS_DIR.mkdir(exist_ok=True)

TALKS_DIR = PROJECT_ROOT / 'talks'
TALKS_DIR.mkdir(exist_ok=True)

from dotenv import load_dotenv
import os

env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)


ENV_USERNAME = os.getenv("ENV_USER")
ENV_PASSWORD = os.getenv("ENV_PASSWORD")
ENV_SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
SCOPUS_AFFILIATION_ID = os.getenv("SCOPUS_AFFILIATION_ID")

from .util import *
from .excel_generator import create_sheet, Cell
from .pubman_base import PubmanBase
from .pubman_creator import PubmanCreator
from .pubman_extractor import PubmanExtractor
from .doi_parser import DOIParser
from .api_manager_scopus import ScopusManager
from .api_manager_crossref import CrossrefManager
