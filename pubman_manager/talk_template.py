from __future__ import annotations

from collections import OrderedDict


TALK_EXTERNAL_LINK_HEADER = "External Link (optional)"

TALK_TEMPLATE_COLUMN_DETAILS = OrderedDict([
    ("Event Name", [35, ""]),
    ("Conference start date\n(dd.mm.YYYY)", [20, ""]),
    ("Conference end date\n(dd.mm.YYYY)", [20, ""]),
    ("Talk date\n(dd.mm.YYYY)", [20, ""]),
    (
        "Conference Location\n(City, Country)",
        [15, "In case of an US-city, please add the State name as well (e.g. New London, NH, USA)"],
    ),
    ("Invited (yes/no)", [15, "Select yes or no"]),
    ("Type (Talk/Poster)", [15, ""]),
    ("Talk Title", [50, ""]),
    (TALK_EXTERNAL_LINK_HEADER, [50, ""]),
    ("Comment (Optional)", [25, ""]),
])

TALK_TEMPLATE_EXAMPLE_FIXED = [
    "deRSE23 - Conference for Research Software Engineering in Germany",
    "20.03.2023",
    "22.03.2023",
    "21.03.2023",
    "Paderborn, Germany",
    "no",
    "",
    "DAMASK: Challenges in collaborative development and outlook",
    "",
    "",
]

TALK_TEMPLATE_DISCLAIMER_TEXT = [
    "Please fill out Talk/Poster details in the same format as the example entry.",
    "",
    "Select author names and affiliations from dropdowns where possible.",
    "If an author is missing from the dropdown list and can't be found with ctrl+f in the 'Names' sheet, write the name manuually in the cell",
    "",
    "The affiliation dropdown list is activated after an author has been selected, please select an item directly from there if possible.",
    "If an affiliation is missing from the dropdown list, please write it manually in the cell.",
    "Affiliations need to follow an exact pattern (<department>, <institution>, <address>) for external authors.",
    "If an MPI-Affiliation is missing, you can look at the 'MPI_Affiliations' sheet for reference (Search with ctrl+f), and just pick the one that matches the group.",
    "If an author has multiple affiliations, add the same author multiple times with different affiliations.",
]
