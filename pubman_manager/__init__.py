from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent

USER_DATA_DIR = PROJECT_ROOT / '.users'
USER_DATA_DIR.mkdir(exist_ok=True)

PUBMAN_CACHE_DIR = USER_DATA_DIR

AUTHORS_INFO_FILE = PUBMAN_CACHE_DIR / 'authors_info.yaml'

FILES_DIR = PROJECT_ROOT / '.files'
FILES_DIR.mkdir(exist_ok=True)

PUBLICATIONS_DIR = PROJECT_ROOT / '.publications'
PUBLICATIONS_DIR.mkdir(exist_ok=True)

TALKS_DIR = PROJECT_ROOT / '.talks'
TALKS_DIR.mkdir(exist_ok=True)

from dotenv import load_dotenv
import os

env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)


ENV_USERNAME = os.getenv("ENV_USER")
ENV_PASSWORD = os.getenv("ENV_PASSWORD")
ENV_USERID = os.getenv("ENV_USERID")
ENV_SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
SCOPUS_AFFILIATION_ID = os.getenv("SCOPUS_AFFILIATION_ID")

from .util import normalize_user_id

def get_user_cache_dir(user_id) -> Path:
    resolved_user_id = normalize_user_id(user_id) if user_id else normalize_user_id(ENV_USERID)
    if not resolved_user_id:
        raise RuntimeError("No user_id available for cache path (missing ENV_USERID).")
    return USER_DATA_DIR / f"user_{resolved_user_id}" / "pubman_cache"

def get_user_dir(user_id) -> Path:
    resolved_user_id = normalize_user_id(user_id) if user_id else normalize_user_id(ENV_USERID)
    if not resolved_user_id:
        raise RuntimeError("No user_id available for user dir (missing ENV_USERID).")
    return USER_DATA_DIR / f"user_{resolved_user_id}"

from .util import *
from .excel_generator import create_sheet, Cell
from .pubman_base import PubmanBase
from .pubman_creator import PubmanCreator
from .pubman_extractor import PubmanExtractor
from .api_manager_scopus import ScopusManager
from .api_manager_crossref import CrossrefManager
from .doi_parser import DOIParser
from .main import generate_author_overview, generate_doi_overview, generate_talks_template, load_user_config, save_user_config, upload_publication_pdfs, refresh_pubman_cache, refresh_pubman_cache_for_user
