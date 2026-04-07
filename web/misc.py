import yaml
from collections import OrderedDict
import os
import tempfile
from flask_mail import Message
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging

from pubman_manager import PubmanBase, PubmanExtractor, create_sheet, TALKS_DIR, USER_DATA_DIR, get_user_cache_dir, get_user_dir
from pubman_manager import generate_doi_overview, refresh_pubman_cache_for_user
from pubman_manager.talk_template import (
    TALK_TEMPLATE_COLUMN_DETAILS,
    TALK_TEMPLATE_DISCLAIMER_TEXT,
    TALK_TEMPLATE_EXAMPLE_FIXED,
)
from pubman_manager.util import normalize_user_id

logger = logging.getLogger(__name__)

extractor = None

def _user_yaml_path(user_id) -> Path:
    return get_user_dir(user_id) / "metadata.yaml"

def update_cache(user_id, org_ids):
    global extractor
    if extractor is None:
        extractor = PubmanExtractor()
    refresh_pubman_cache_for_user(user_id, org_ids)
    cache_dir = get_user_cache_dir(user_id)
    with open(cache_dir / 'authors_info.yaml', 'r', encoding='utf-8') as f:
        authors_info = yaml.load(f, Loader=yaml.FullLoader)
    def _omit_affiliation(affiliation: str) -> bool:
        return "eisenforschung" in str(affiliation).casefold()
    names_affiliations = OrderedDict()
    for key, val in (authors_info or {}).items():
        if not val:
            continue
        counts = val.get("affiliation_counts") if isinstance(val, dict) else None
        if isinstance(counts, dict):
            filtered = {aff: count for aff, count in counts.items() if not _omit_affiliation(aff)}
            names_affiliations[key] = filtered
    file_path = TALKS_DIR / f"Template_Talks_{org_ids[0]}.xlsx"
    n_authors = 80
    column_details = TALK_TEMPLATE_COLUMN_DETAILS.copy()
    example_fixed = list(TALK_TEMPLATE_EXAMPLE_FIXED)
    example_names = [
        ("Daniel Otto de Mentock", "Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society"),
        ("Sharan Roongta", "Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society"),
        ("Philip Eisenlohr", "Michigan State University, Chemical Engineering and Materials Science, East Lansing, MI 48824, USA"),
        ("Martin Diehl", "Department of Computer Science, KU Leuven, Celestijnenlaan 200 A, Leuven 3001, Belgium"),
        ("Franz Roters", "Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society"),
    ]
    example_row = list(example_fixed)
    for i in range(n_authors):
        if i < len(example_names):
            name, affiliation = example_names[i]
            if _omit_affiliation(affiliation):
                affiliation = ""
        else:
            name, affiliation = "", ""
        example_row.extend([name, affiliation])
    create_sheet(
        file_path,
        names_affiliations,
        column_details,
        n_authors,
        "Event Name",
        n_entries=45,
        example_row=example_row,
        freeze_first_n_cols=0,
        disclaimer_text=TALK_TEMPLATE_DISCLAIMER_TEXT,
    )

def get_file_for_dois(dois, doi_parser):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    generate_doi_overview(dois, output_path=Path(temp_file.name))
    return temp_file.name

def parse_new_publications(doi_parser):
    author_publications = {}
    dois_by_user = {}
    for user_yaml_path in USER_DATA_DIR.glob("*/metadata.yaml"):
        user_id = user_yaml_path.parent.name
        dois_by_user[user_id] = get_user_dois(user_id, doi_parser, author_publications=author_publications)

def get_user_dois(user_id, doi_parser, author_publications=None, force: bool = False):
    if author_publications is None:
        author_publications = {}
    new_dois = set()
    user_yaml_path = _user_yaml_path(user_id)
    if not user_yaml_path.exists():
        return new_dois
    with user_yaml_path.open("r", encoding="utf-8") as f:
        user_data = yaml.safe_load(f) or []
    if isinstance(user_data, dict):
        tracked_authors = user_data.get("tracked_authors", [])
    else:
        tracked_authors = user_data
    ignored_dois = set(user_data.get("ignored_dois", []) if isinstance(user_data, dict) else [])
    cache_path = get_user_dir(user_id) / "publication_collection_history.yaml"
    cached_dois = set()
    if cache_path.exists() and not force:
        with cache_path.open("r", encoding="utf-8") as f:
            cache_data = yaml.safe_load(f) or {}
        for entry in cache_data.values():
            if isinstance(entry, list):
                cached_dois.update(entry)

    for tracked_author in tracked_authors:
        if tracked_author not in author_publications:
            author_publications[tracked_author] = doi_parser.get_dois_for_author(
                tracked_author,
                pubyear_start=2024,
                processed_dois=cached_dois if not force else None,
            )
        df = author_publications[tracked_author]
        if df.empty:
            logger.warning(f"No data found for author {tracked_author}. Skipping...")
            continue
        new_dois.update(
            df.loc[
                ~df['DOI'].isin(ignored_dois) &
                (df['Field'].isnull() | (df['Field'] == "")), 'DOI'].tolist()
        )
    return new_dois

def send_test_mail_(target):
    sender_email = 'pubman_manager@mpie.de'
    recipient_email = target
    subject = "Test Email"
    body = "test email sent from pubman_manager"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    smtp_server = "xmail1.mpie.de"
    smtp_port = 25
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(sender_email, recipient_email, msg.as_string())

def send_author_publications(new_publication_dois, user_email, doi_parser):
    temp_file_path = get_file_for_dois(new_publication_dois, doi_parser)

    sender_email = 'pubman_manager@mpie.de'
    recipient_email = user_email
    subject = "Publication Update"
    body = f"New publications to import to pubman_manager"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    if temp_file_path and os.path.exists(temp_file_path):
        with open(temp_file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(temp_file_path)}",
        )
        msg.attach(part)
    else:
        logger.info(f"Attachment file {temp_file_path} not found or invalid.")
    smtp_server = "xmail1.mpie.de"
    smtp_port = 25
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(sender_email, recipient_email, msg.as_string())
    logger.info("Email sent successfully.")

def send_publications():
    pass

def run_periodic_task():
    for user_yaml_path in USER_DATA_DIR.glob("*/metadata.yaml"):
        user_id = user_yaml_path.parent.name
        with user_yaml_path.open("r", encoding="utf-8") as f:
            user_info = yaml.safe_load(f) or {}
        if not isinstance(user_info, dict):
            continue
        org_ids = user_info.get("department_org_ids", [])
        if org_ids:
            update_cache(user_id, org_ids)

        pubman_api = PubmanBase()
        parser = DOIParser(pubman_api)
        new_publication_dois = get_user_dois(user_id, parser)
        if new_publication_dois:
            logging.info(f'Processing new DOIS for user {user_id} ({user_info}):')
            logging.info(f'{new_publication_dois}')
            email = user_info.get("email")
            if email:
                send_author_publications(new_publication_dois, email, parser)
        else:
            logging.info(f"No new DOIS for user {user_id} (tracking {user_info.get('tracked_authors', [])})")
