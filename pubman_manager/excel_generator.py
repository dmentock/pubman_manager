import xlsxwriter
from collections import OrderedDict, Counter
import pandas as pd

from typing import *

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

def create_sheet(
    file_path: str,
    affiliations_by_name_pubman: Dict[Tuple[str, str], List[str]],
    column_details: OrderedDict[str, str],
    n_authors: int,
    prefill_publications: Dict[str, Cell] = None,
    n_entries: int = None,
    example_row: List[str] = None,
):
    if prefill_publications is None and n_entries is None:
        raise ValueError("Either prefill_publications or n_entries must be provided to determine the number of rows.")

    if prefill_publications is not None:
        n_entries = len(prefill_publications) + 1

    col_layout = OrderedDict([('Entry Number', (10, ''))])
    col_layout.update(column_details)

    for i in range(n_authors):
        col_layout[f'Author {i + 1}'] = (20, '')
        col_layout[f'Affiliation {i + 1}'] = (35, '')

    for i in range(n_authors):
        col_layout[f'Helper {i + 1}'] = (10, '')

    workbook = xlsxwriter.Workbook(file_path)

    main_sheet = workbook.add_worksheet("MainSheet")

    names_sheet = workbook.add_worksheet("Names")
    names = list(affiliations_by_name_pubman.keys())
    for row_index, (first_name, last_name) in enumerate(names):
        for col_index in range(n_authors):
            col = list(col_layout.keys()).index(f'Author {col_index + 1}')
            main_sheet.write(row_index, col, first_name + ' ' + last_name)
    for row_index in range(len(names)):
        main_sheet.set_row(row_index, None, None, {'hidden': True})

    # mpi_authors_sheet = workbook.add_worksheet("MPI_Authors")
    mpi_affiliations_sheet = workbook.add_worksheet("MPI_Affiliations")
    mpi_affiliations = Counter()
    # mpi_authors = set()
    for (first_name, last_name) in names:
        for affiliation in affiliations_by_name_pubman[(first_name, last_name)]:
            if any(keyword in affiliation for keyword in ['Max Planck', 'Max-Planck']):
                mpi_affiliations[affiliation] += 1
                # mpi_authors.add(f'{first_name} {last_name}')
    # for row_index, name in enumerate(sorted(list(mpi_authors))):
    #     mpi_authors_sheet.write(row_index, 0, name)
    mpi_affiliations = [affiliation for affiliation, count in mpi_affiliations.most_common()]
    for row_index, affiliation in enumerate(mpi_affiliations):
        mpi_affiliations_sheet.write(row_index, 0, affiliation)
    if len(mpi_affiliations) > 20:
        mpi_affiliations = mpi_affiliations[:20]
    for row_index, affiliation in enumerate(mpi_affiliations):
        mpi_affiliations_sheet.write(row_index, 0, affiliation)

    header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter', 'locked': True})
    italic_format = workbook.add_format({'italic': True, 'text_wrap': True, 'locked': True})
    wrap_format = workbook.add_format({'text_wrap': True, 'locked': True})
    text_format = workbook.add_format({'num_format': '@'})  # '@' forces text format in Excel

    disclaimer_format = workbook.add_format({
        'bold': True,
        'font_color': 'red',
        'text_wrap': True,
        'valign': 'vcenter'
    })

    colors = {
        'GRAY': '#d3d3d3',
        'RED': '#ff9999',
        'PURPLE': '#e6e6fa',
        'GREEN': '#ccffcc'
    }

    cell_color_formats = {color: workbook.add_format({'bg_color': colors[color], 'text_wrap': True}) for color in colors.keys()}

    disclaimer_lines = [
        "Please try to avoid copy-pasting in areas with dropdowns, as this will break the underlying data validation and make subsequent editing difficult.",
        "Only do so if you are sure you won't have to change the author name or affiliations afterwards, e.g. when pasting from identical sections from previous entries."
    ]

    disclaimer_start_row = len(names) + 1
    for i, line in enumerate(disclaimer_lines):
        main_sheet.merge_range(
            f'A{disclaimer_start_row + i}:{col_num_to_col_letter(len(col_layout))}{disclaimer_start_row + i}',
            line, disclaimer_format
        )
    start_row = disclaimer_start_row + len(disclaimer_lines) + 2

    for col_index, (header, (width, comment)) in enumerate(col_layout.items()):
        main_sheet.write(start_row, col_index, header, header_format)
        if header == 'Entry Number' and example_row is not None:
            main_sheet.write(start_row + 1, col_index, 'Example', italic_format)
            for i, col_entry in enumerate(example_row):
                main_sheet.write(start_row + 1, col_index + 1 + i, col_entry, italic_format)
        main_sheet.set_column(col_index, col_index, width)

    start_row = start_row + len(disclaimer_lines)
    names_sheet.write('A1', 'Names')
    names_sheet.write('B1', 'Affiliations')
    for row_index, (first_name, last_name) in enumerate(names, start=1):
        names_sheet.write(row_index, 0, first_name + ' ' + last_name)
        for col_index, affiliation in enumerate(affiliations_by_name_pubman[(first_name, last_name)], start=1):
            names_sheet.write(row_index, col_index, affiliation)


    for entry_idx in range(n_entries):
        for i in range(n_authors):
            name_col = list(col_layout.keys()).index(f'Author {i + 1}')
            helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
            affiliation_col = list(col_layout.keys()).index(f'Affiliation {i + 1}')
            row_index_cell = f'{col_num_to_col_letter(helper_col + 1)}{start_row + entry_idx + 1}'

            author_explanation = 'Write the name in <first_name> <last_name> format\n\n' + \
                'If it is missing and you cannot find it in the "Names" sheet with ctrl+f (check for usage of . or - or middle names/abbreviations)' + \
                ', enter it yourself and select "yes" to override data validation.'
            main_sheet.data_validation(start_row + entry_idx, name_col, start_row + entry_idx, name_col, {
                'validate': 'list',
                'source': f'Names!$A$2:$A${len(names) + 1}',
                'input_message': author_explanation,
                'error_type': 'warning'
            })

            main_sheet.write_formula(start_row + entry_idx, helper_col,
                                     f'MATCH({col_num_to_col_letter(name_col + 1)}{start_row + entry_idx + 1}, Names!$A$2:$A${len(names) + 1}, 0) + 1')

            affiliation_col = list(col_layout.keys()).index(f'Affiliation {i + 1}')
            helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')

            affiliation_explanation = 'Select Affiliation from the list\n\n' + \
                                  'If not available, enter it yourself and override data validation.\n' + \
                                  'If there are multiple affiliations, add the same author multiple times.\n' + \
                                  'If the same affiliation appears more than once, just select any'
            main_sheet.data_validation(start_row + entry_idx, affiliation_col, start_row + entry_idx, affiliation_col, {
                'validate': 'list',
                'source': f'=INDIRECT("Names!B" & {row_index_cell} & ":I" & {row_index_cell})',
                'input_message': affiliation_explanation,
                'error_type': 'warning'
            })

            if entry_idx == 0:
                # main_sheet.data_validation(start_row - 3, name_col, start_row - 3, name_col, {
                #     'validate': 'list',
                #     'source': f'MPI_Authors!$A$1:$A${len(mpi_authors)+1}',
                #     'input_message': 'Browse list of MPI Authors',
                #     'error_type': 'warning'
                # })
                # main_sheet.write(start_row - 3, affiliation_col, '▼ MPI Authors')

                main_sheet.data_validation(start_row - 3, affiliation_col, start_row - 3, affiliation_col, {
                    'validate': 'list',
                    'source': f'MPI_Affiliations!$A$1:$A${len(mpi_affiliations)+1}',
                    'input_message': 'Browse list of MPI Affiliations',
                    'error_type': 'warning'
                })
                main_sheet.write(start_row - 3, affiliation_col, '▼ Common MPI Affiliations')

            if prefill_publications and entry_idx > 0:
                comment = prefill_publications[entry_idx-1].get(f'Affiliation {i + 1}', Cell('')).comment
                main_sheet.write_comment(start_row + entry_idx, affiliation_col, comment)

    entry_number_col = list(col_layout.keys()).index('Entry Number')
    for entry_idx in range(n_entries):
        if entry_idx > 0:
            main_sheet.write(start_row + entry_idx, entry_number_col, entry_idx)

    if prefill_publications:
        for entry_idx, publication in enumerate(prefill_publications):
            for col_index, header in enumerate(col_layout.keys()):
                if header in publication:
                    cell_obj = publication[header]
                    if cell_obj:
                        # Ensure cell_value is retrieved properly
                        cell_value = cell_obj.data if cell_obj.data is not None else ''

                        # Check if we need to force text format
                        cell_format = cell_color_formats.get(cell_obj.color, wrap_format)
                        if isinstance(cell_obj, Cell) and cell_obj.force_text:
                            main_sheet.write_string(start_row + 1 + entry_idx, col_index, str(cell_value), text_format)
                        else:
                            main_sheet.write(start_row + 1 + entry_idx, col_index, cell_value, cell_format)
    else:
        for entry_idx in range(n_entries):
            for col_index, (header, (width, comment)) in enumerate(col_layout.items()):
                if 'Helper' not in header and header not in ['Entry Number']:
                    main_sheet.write(start_row + entry_idx, col_index, '', wrap_format)
                if comment:
                    main_sheet.data_validation(
                        start_row + entry_idx, col_index, start_row + entry_idx, col_index,
                        {
                            'validate': 'any',
                            'input_message': comment,
                            'error_type': 'warning'
                        }
                    )

    for i in range(n_authors):
        helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
        main_sheet.set_column(helper_col, helper_col, None, None, {'hidden': True})
    workbook.close()

def col_num_to_col_letter(col_num):
    """Convert a column number (1-indexed) to a column letter."""
    letter = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter
