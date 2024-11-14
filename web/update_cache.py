import yaml
from collections import OrderedDict

from pubman_manager import PubmanExtractor, create_sheet, PUBMAN_CACHE_DIR, TALKS_DIR
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

    # Create the Excel file
    create_sheet(file_path, names_affiliations, column_details, n_authors, n_entries=45)