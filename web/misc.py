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

from pubman_manager import PubmanBase, PubmanExtractor, DOIParser, create_sheet, PUBMAN_CACHE_DIR, TALKS_DIR

logger = logging.getLogger(__name__)

extractor = PubmanExtractor()

def update_cache(org_id):
    extractor.extract_org_data(org_id)
    with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r', encoding='utf-8') as f:
        authors_info = yaml.load(f, Loader=yaml.FullLoader)
    names_affiliations = OrderedDict({key: val['affiliations'] for key, val in authors_info.items() if val})
    file_path = TALKS_DIR / f"Template_Talks_{org_id}.xlsx"
    n_authors = 80
    column_details = OrderedDict([
        ('Event Name', [35, '']),
        ('Conference start date\n(dd.mm.YYYY)', [20, '']),
        ('Conference end date\n(dd.mm.YYYY)', [20, '']),
        ('Talk date\n(dd.mm.YYYY)', [20, '']),
        ('Conference Location\n(City, Country)', [15, 'In case of an US-city, please add the State name as well (e.g. New London, NH, USA)']),
        ('Invited (y/n)', [15, '']),
        ('Type (Talk/Poster)', [15, '']),
        ('Talk Title', [50, '']),
        ('Comment (Optional)', [25, '']),
    ])
    create_sheet(file_path, names_affiliations, column_details, n_authors, n_entries=45)

def get_file_for_dois(dois, doi_parser):
    df_dois_overview = doi_parser.filter_dois(dois)
    dois_data = doi_parser.collect_data_for_dois(df_dois_overview, force=True)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    doi_parser.write_dois_data(temp_file.name, dois_data)
    return temp_file.name

def parse_new_publications(doi_parser):
    with open(Path(__file__).parent / 'users.yaml', 'r') as f:
        users = yaml.safe_load(f)

    author_publications = {}
    dois_by_user = {}
    for user in users:
        dois_by_user[user] = get_user_dois(doi_parser, author_publications=author_publications)

def get_user_dois(user_id, doi_parser, author_publications=None):
    with open(Path(__file__).parent / 'users.yaml', 'r') as f:
        users = yaml.safe_load(f)
    if author_publications is None:
        author_publications = {}
    new_dois = set()
    for tracked_author in users[user_id]['tracked_authors']:
        if tracked_author not in author_publications:
            author_publications[tracked_author] = doi_parser.get_dois_for_author(tracked_author, pubyear_start=2024)
        df = author_publications[tracked_author]
        if df.empty:
            logger.warning(f"No data found for author {tracked_author}. Skipping...")
            continue
        new_dois.update(
            df.loc[
                ~df['DOI'].isin(users[user_id].get('ignored_dois', [])) &
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
    with open(Path(__file__).parent / 'users.yaml', 'r') as f:
        users = yaml.safe_load(f)
    for org_id in {users[user_id]['org_id'] for user_id in users.keys()}:
        update_cache(org_id)

    for user_id, user_info in users.items():
        pubman_api = PubmanBase()
        parser = DOIParser(pubman_api)
        new_publication_dois = get_user_dois(user_id, parser)
        if new_publication_dois:
            logging.info(f'Processing new DOIS for user {user_id} ({user_info}):')
            logging.info(f'{new_publication_dois}')
            send_author_publications(new_publication_dois, user_info['email'], parser)
        else:
            logging.info(f"No new DOIS for user {user_info['email']} (tracking {user_info['tracked_authors']})")
