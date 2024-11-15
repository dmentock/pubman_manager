import yaml
from collections import OrderedDict
import os
import tempfile
from flask_mail import Message
from pathlib import Path

from pubman_manager import PubmanExtractor, DOIParser, create_sheet, PUBMAN_CACHE_DIR, TALKS_DIR

extractor = PubmanExtractor()

def update_cache(org_id):
    extractor.extract_org_data(org_id)
    with open(PUBMAN_CACHE_DIR / org_id / 'authors_info.yaml', 'r', encoding='utf-8') as f:
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

def parse_new_publications(email_client, doi_parser):
    with open(Path(__file__).parent / 'users.yaml', 'r') as f:
        users = yaml.safe_load(f)

    author_publications = {}
    for author in {author for authors in [users[user]['tracked_authors'] for user in users.keys()] for author in authors}:
        author_publications[author] = doi_parser.get_dois_for_author(author, pubyear_start=2024)

    for user in users:
        new_dois = []
        for tracked_author in users[user]['tracked_authors']:
            df = author_publications[tracked_author]
            new_dois.extend(
                df.loc[
                    ~df['DOI'].isin(users[user].get('ignored_dois', [])) &
                    (df['Field'].isnull() | (df['Field'] == ""))
                , 'DOI'].tolist()
            )

        if new_dois:
            users[user]['past_dois'] = users[user].get('past_dois', []) + new_dois
            # Process new DOIs
            df_dois_overview = doi_parser.filter_dois(new_dois)
            dois_data = doi_parser.collect_data_for_dois(df_dois_overview, force=True)

            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            try:
                # Write the data to the file
                doi_parser.write_dois_data(temp_file.name, dois_data)

                # Prepare and send email
                email = users[user].get("email")  # Retrieve user email from users.yaml
                if not email:
                    print(f"No email configured for user {user}")
                    continue

                msg = Message(
                    subject="New Publications",
                    sender=doi_parser.pubman_api.user_email,
                    recipients=[email],
                    body=f"Dear {user},\n\nNew publications related to your tracked authors have been found. Please revise the attached file and upload it to ___ \n\nBest regards"
                )
                with open(temp_file.name, "rb") as attachment:
                    msg.attach(
                        filename=os.path.basename(temp_file.name),
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        data=attachment.read()
                    )
                email_client.send(msg)
                print(f"Email sent to {email} with new DOIs for {user}.")
            finally:
                os.unlink(temp_file.name)
        else:
            print(f"No new DOIs for user {user}")
    with open(Path(__file__).parent / 'users.yaml', 'w') as f:
        yaml.dump(users, f)