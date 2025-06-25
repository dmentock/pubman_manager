import pytest
import pandas as pd
from unittest.mock import patch
from pubman_manager import PubmanBase
from pathlib import Path
from pubman_manager import create_sheet
from collections import OrderedDict
def test_create_sheet():
    column_details = OrderedDict([('Title', [50, '']), ('Type', [20, '']), ('Journal Title', [25, '']), ('Volume', [15, '']), ('Page', [10, '']), ('Publisher', [25, '']), ('ISSN', [20, '']), ('DOI', [20, '']), ('Date created', [20, '']), ('Date issued', [20, '']), ('Date published', [20, ''])])
    prefill_metadata = [OrderedDict([('Title', ['Interstitial Segregation has the Potential to Mitigate Liquid Metal Embrittlement in Iron', 50, '']), ('Type', ['journal-article', 20, '']), ('Journal Title', ['Advanced Materials', 25, '']), ('Volume', ['35', 15, '']), ('Page', [None, 10, '']), ('Publisher', ['Wiley', 25, '']), ('ISSN', ['0935-9648', 20, '']), ('DOI', ['10.1002/adma.202211796', 20, '']), ('Date created', ['09.04.2023', 20, '']), ('Date issued', [None, 20, '']), ('Date published', [None, 20, ''])])]
    prefill_Names = [OrderedDict([('Name 1', ['Ali Ahmadian', '']), ('Affiliation 1', ['Max-Planck-Institut fuer Eisenforschung GmbH  40237 Dusseldorf Germany', '']), ('Name 2', ['Daniel Scheiber', '']), ('Affiliation 2', ['Materials Center Leoben GmbH  Leoben 8700 Austria', '']), ('Name 3', ['Xuyang Zhou', '']), ('Affiliation 3', ['Max-Planck-Institut fuer Eisenforschung GmbH  40237 Dusseldorf Germany', '']), ('Name 4', ['Baptiste Gault', '']), ('Affiliation 4', ['Max-Planck-Institut fuer Eisenforschung GmbH  40237 Dusseldorf Germany', '']), ('Name 5', ['Baptiste Gault', '']), ('Affiliation 5', ['Department of Materials, Royal School of Mines Imperial College London  London UK', '']), ('Name 6', ['Lorenz Romaner', '']), ('Affiliation 6', ['Materials Center Leoben GmbH  Leoben 8700 Austria', '']), ('Name 7', ['Lorenz Romaner', '']), ('Affiliation 7', ['Montanuniversitat Leoben  Leoben 8700 Austria', '']), ('Name 8', ['Reza D. Kamachali', '']), ('Affiliation 8', ['Federal Institute for Materials Research and Testing (BAM) Unter den Eichen 87  12205 Berlin Germany', '']), ('Name 9', ['Werner Ecker', '']), ('Affiliation 9', ['Materials Center Leoben GmbH  Leoben 8700 Austria', '']), ('Name 10', ['Gerhard Dehm', '']), ('Affiliation 10', ['Max-Planck-Institut fuer Eisenforschung GmbH  40237 Dusseldorf Germany', '']), ('Name 11', ['Christian H. Liebscher', '']), ('Affiliation 11', ['Max-Planck-Institut fuer Eisenforschung GmbH  40237 Dusseldorf Germany', ''])])]
    create_sheet(f'./test.xlsx', {}, column_details, 20, 20)

if __name__ == "__main__":
    pytest.main(["-v"])
