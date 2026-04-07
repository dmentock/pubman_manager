import xlsxwriter
from collections import OrderedDict, Counter
from typing import *
import pandas as pd


class Cell:
    def __init__(self, data, width=None, color='', comment='', compare_error=None, force_text=False):
        self.data = '' if not data or pd.isna(data) else data
        self.width = width
        self.color = color
        self.comment = comment
        self.compare_error = compare_error
        self.force_text = force_text

    def __str__(self):
        return f"Cell(data={self.data}, color={self.color}, comment={self.comment}, compare_error={self.compare_error}, force_text={self.force_text})"

    def __repr__(self):
        return str(self)


def col_num_to_col_letter(col_num: int) -> str:
    letters = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def create_sheet(
    file_path: str,
    affiliations_by_name_pubman: Dict[Tuple[str, str], Dict[str, int]],
    column_details: "OrderedDict[str, Tuple[int, str]]",
    n_authors: int,
    header_name: str,
    prefill_publications: Optional[List[Dict[str, Cell]]] = None,
    n_entries: Optional[int] = None,
    example_row: Optional[List[str]] = None,
    freeze_first_n_cols: int = 1,
    disclaimer_text: Optional[List[str]] = None,
):
    if prefill_publications is None and n_entries is None:
        raise ValueError("Either prefill_publications or n_entries must be provided.")
    if prefill_publications is not None:
        n_entries = len(prefill_publications)
    assert n_entries is not None

    fixed_columns = OrderedDict(column_details)
    author_headers = [f"Author {i+1}" for i in range(n_authors)]
    affiliation_headers = [f"Affiliation {i+1}" for i in range(n_authors)]
    helper_headers = [f"Helper {i+1}" for i in range(n_authors)]

    alternating_columns = OrderedDict(fixed_columns)
    for i in range(n_authors):
        alternating_columns[author_headers[i]] = (20, '')
        alternating_columns[affiliation_headers[i]] = (35, '')
        alternating_columns[helper_headers[i]] = (10, '')

    headers = list(alternating_columns.keys())
    header_to_index = {h: i for i, h in enumerate(headers)}
    visible_headers = [h for h in headers if not h.startswith('Helper ')]

    workbook_target = file_path
    workbook_options: dict[str, Any] = {}
    if hasattr(file_path, "write"):
        workbook_options["in_memory"] = True
    else:
        workbook_target = str(file_path)

    workbook = xlsxwriter.Workbook(workbook_target, workbook_options)
    sheet_main = workbook.add_worksheet("MainSheet")
    sheet_names = workbook.add_worksheet("Names")
    sheet_mpi = workbook.add_worksheet("MPI_Affiliations")

    fmt_header = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter', 'locked': True})
    fmt_italic = workbook.add_format({'italic': True, 'text_wrap': True, 'locked': True})
    fmt_wrap = workbook.add_format({'text_wrap': True, 'locked': True})
    fmt_text = workbook.add_format({'num_format': '@'})
    fmt_disclaimer = workbook.add_format({'bold': True, 'font_color': 'red', 'text_wrap': True, 'valign': 'vcenter'})

    color_map = {'GRAY': '#696969', 'RED': '#ff9999', 'PURPLE': '#e6e6fa', 'GREEN': '#ccffcc', 'ORANGE': 'orange'}
    fmt_bg = {k: workbook.add_format({'bg_color': v, 'text_wrap': True}) for k, v in color_map.items()}

    name_pairs = list(affiliations_by_name_pubman.keys())
    full_names = [f"{fn} {ln}" for fn, ln in name_pairs]

    sheet_names.write('A1', 'Names')
    sheet_names.write('B1', 'Affiliations')
    for row_idx, (fn, ln) in enumerate(name_pairs, start=1):
        sheet_names.write(row_idx, 0, f"{fn} {ln}")
        for col_idx, aff in enumerate(sorted(affiliations_by_name_pubman[(fn, ln)].keys(), key=lambda x: affiliations_by_name_pubman[(fn, ln)][x], reverse=True), start=1):
            sheet_names.write(row_idx, col_idx, aff)

    for hidden_row_index, display_name in enumerate(full_names):
        for author_header in author_headers:
            sheet_main.write(hidden_row_index, header_to_index[author_header], display_name)
        sheet_main.set_row(hidden_row_index, None, None, {'hidden': True})

    mpi_counts = Counter()
    for fn, ln in name_pairs:
        for aff in affiliations_by_name_pubman[(fn, ln)].keys():
            if ('Max Planck' in aff) or ('Max-Planck' in aff):
                mpi_counts[aff] += 1
    mpi_sorted = [aff for aff, _ in mpi_counts.most_common()]
    mpi_top = mpi_sorted[:20]
    for r, aff in enumerate(mpi_sorted):
        sheet_mpi.write(r, 0, aff)
    for r, aff in enumerate(mpi_top):
        sheet_mpi.write(r, 1, aff)

    for col_index, (header, (width, _tooltip)) in enumerate(alternating_columns.items()):
        sheet_main.set_column(col_index, col_index, width)

    if disclaimer_text is None:
        disclaimer_text = [
            "Please fill out Talk/Poster details in the same format as the example entry.",
            "Select author names and affiliations from dropdowns where possible.",
            "If an author is missing from the dropdown list and can't be found with ctrl+f in the 'Names' sheet, write the name manuually in the cell",
            "If an affiliation is missing from the dropdown list, write it manually in the cell.",
            "Affiliations need to follow an exact pattern (<department>, <institution>, <address>) for external or ",
            "(<group_name>, <department>, <institute>) for MPI authors.",
            "If an author has multiple affiliations, add the same author multiple times with different affiliations.",
            "",
            "Please see the example affiliations or the 'MPI_Affiliations' sheet for further reference.",
        ]

    hidden_rows_count = len(full_names)
    row_disclaimer_start = hidden_rows_count + 1
    row_disclaimer_end = row_disclaimer_start + len(disclaimer_text) - 1
    row_header = row_disclaimer_end + 2
    row_example = row_header + 1 if example_row else None
    row_data_start = row_example if example_row else (row_header + 1)
    data_rows_including_example = n_entries + (1 if example_row else 0)
    row_data_end = row_data_start + data_rows_including_example - 1

    sheet_main.freeze_panes(row_data_start, freeze_first_n_cols)

    last_col_letter = col_num_to_col_letter(len(headers))
    for i, line in enumerate(disclaimer_text):
        r = row_disclaimer_start + i
        sheet_main.merge_range(f"A{r+1}:{last_col_letter}{r+1}", line, fmt_disclaimer)

    for col_index, header in enumerate(headers):
        sheet_main.write(row_header, col_index, header, fmt_header)

    example_values_by_header = dict(zip(visible_headers[:len(example_row or [])], example_row or []))
    if example_values_by_header:
        for header, value in example_values_by_header.items():
            sheet_main.write(row_example, header_to_index[header], value, fmt_italic)

    if n_authors > 0:
        col_aff1 = header_to_index['Affiliation 1']
        sheet_main.data_validation(row_header - 1, col_aff1, row_header - 1, col_aff1, {
            'validate': 'list',
            'source': f'MPI_Affiliations!$B$1:$B${len(mpi_top)}',
            'input_message': 'Browse list of MPI Affiliations',
            'error_type': 'warning'
        })
        sheet_main.write(row_header - 1, col_aff1, '▼ Common MPI Affiliations')

    def add_author_affiliation_validation(target_row: int, author_index_1based: int):
        col_author = header_to_index[f"Author {author_index_1based}"]
        col_helper = header_to_index[f"Helper {author_index_1based}"]
        col_aff = header_to_index[f"Affiliation {author_index_1based}"]

        author_prompt = (
            'Write the name in <first_name> <last_name> format\n\n'
            'If it is missing and you cannot find it in the "Names" sheet with ctrl+f (check for usage of . or - or middle names/abbreviations), '
            'enter it yourself and select "yes" to override data validation.'
        )
        sheet_main.data_validation(target_row, col_author, target_row, col_author, {
            'validate': 'list',
            'source': f'Names!$A$2:$A${len(full_names) + 1}',
            'input_message': author_prompt,
            'error_type': 'warning'
        })

        a1_helper = f'{col_num_to_col_letter(col_helper + 1)}{target_row + 1}'
        a1_author = f'{col_num_to_col_letter(col_author + 1)}{target_row + 1}'
        sheet_main.write_formula(target_row, col_helper, f'MATCH({a1_author}, Names!$A$2:$A${len(full_names) + 1}, 0) + 1')

        affiliation_prompt = (
            'Select Affiliation from the list\n\n'
            'If not available, enter it yourself and override data validation.\n'
            'If there are multiple affiliations, add the same author multiple times.\n'
            'If the same affiliation appears more than once, just select any.'
        )
        sheet_main.data_validation(target_row, col_aff, target_row, col_aff, {
            'validate': 'list',
            'source': f'=INDIRECT("Names!B" & {a1_helper} & ":ZZ" & {a1_helper})',
            'input_message': affiliation_prompt,
            'error_type': 'warning'
        })

    for row in range(row_data_start, row_data_end + 1):
        for header, (_width, tooltip) in alternating_columns.items():
            col = header_to_index[header]
            is_example_row = (row == row_example)
            is_example_cell = is_example_row and (header in example_values_by_header)
            if (not header.startswith('Helper ')) and (not is_example_cell):
                sheet_main.write(row, col, '', fmt_wrap)
            if tooltip and header != 'Invited (yes/no)':
                sheet_main.data_validation(row, col, row, col, {
                    'validate': 'any',
                    'input_message': tooltip,
                    'error_type': 'warning'
                })
        if 'Invited (yes/no)' in header_to_index:
            invited_col = header_to_index['Invited (yes/no)']
            sheet_main.data_validation(row, invited_col, row, invited_col, {
                'validate': 'list',
                'source': ['yes', 'no'],
                'input_message': 'Select yes or no',
                'error_type': 'warning'
            })
        for k in range(1, n_authors + 1):
            add_author_affiliation_validation(row, k)

    if prefill_publications:
        first_prefill_row = row_data_start + (1 if example_values_by_header else 0)
        for offset, publication in enumerate(prefill_publications):
            r = first_prefill_row + offset
            for key, cell in publication.items():
                if key not in header_to_index:
                    continue
                c = header_to_index[key]
                if isinstance(cell, Cell):
                    value = '' if cell.data is None else cell.data
                    fmt = fmt_bg.get(cell.color, fmt_wrap)
                    if cell.force_text:
                        sheet_main.write_string(r, c, str(value), fmt_text)
                    else:
                        sheet_main.write(r, c, value, fmt)
                    if cell.comment:
                        sheet_main.write_comment(r, c, cell.comment)
                else:
                    sheet_main.write(r, c, '' if cell is None else cell, fmt_wrap)

    for helper in helper_headers:
        col = header_to_index[helper]
        sheet_main.set_column(col, col, None, None, {'hidden': True})

    workbook.close()
