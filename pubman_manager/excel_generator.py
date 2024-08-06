import pandas as pd
import xlsxwriter
from collections import OrderedDict
from pathlib import Path
import yaml

def create_sheet(file_path, names_affiliations, column_details, n_authors, prefill_publications=None, n_entries=None):
    # Ensure at least one of prefill_publications or n_entries is provided
    if prefill_publications is None and n_entries is None:
        raise ValueError("Either prefill_publications or n_entries must be provided to determine the number of rows.")

    # If prefill_publications is provided, use its length as n_entries
    if prefill_publications is not None:
        n_entries = len(prefill_publications)

    # Define the column layout
    col_layout = OrderedDict([('Entry Number', (10, ''))])
    col_layout.update(column_details)  # Set a default width for columns

    for i in range(n_authors):
        col_layout[f'Author {i + 1}'] = (20, '')
        col_layout[f'Affiliation {i + 1}'] = (35, '')

    # Add helper columns to the end of the layout
    for i in range(n_authors):
        col_layout[f'Helper {i + 1}'] = (10, '')  # Add helper columns at the end

    # Create a workbook and add worksheets
    workbook = xlsxwriter.Workbook(file_path)
    main_sheet = workbook.add_worksheet("MainSheet")
    names_sheet = workbook.add_worksheet("Names")

    # Create formats for the headers, italic text, and wrapped text
    header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter'})
    italic_format = workbook.add_format({'italic': True, 'text_wrap': True})
    wrap_format = workbook.add_format({'text_wrap': True})

    colors = {
        'gray': '#d3d3d3',
        'yellow': '#ffffe0',
        'orange': '#ffd8b1',
        'red': '#ff9999',
        'purple': '#e6e6fa'
    }

    color_messages = {
        'gray': 'Author or similar Affiliation not found in database -> using affiliation from publisher (err>0.2)',
        'yellow': 'Author found in database -> using most similar affiliation to the one provided by the publisher (err<=0.2)',
        'orange': 'Author found in database, but no affiliation provided by publisher -> guessing affiliation based on publication title',
        'red': 'Author not found in database, no affiliation provided by publisher -> guessing based on other author affiliations (needs revision)',
        'purple': 'Generic Max Planck Affiliation was provided by publisher, but the precise group was not found in the database (needs revision)'
    }

    cell_color_formats = {color: workbook.add_format({'bg_color': colors[color], 'text_wrap': True}) for color in colors.keys()}

    # Extract names and affiliations
    names = list(names_affiliations.keys())
    affiliations = names_affiliations

    # Write names in the rows above the header for autocomplete
    for row_index, name in enumerate(names):
        for col_index in range(n_authors):
            col = list(col_layout.keys()).index(f'Author {col_index + 1}')
            main_sheet.write(row_index, col, name)

    # Hide those rows
    for row_index in range(len(names)):
        main_sheet.set_row(row_index, None, None, {'hidden': True})

    header_row = len(names)
    # Write the headers
    for col_index, (header, (width, tooltip)) in enumerate(col_layout.items()):
        main_sheet.write(header_row, col_index, header, header_format)
        main_sheet.write(header_row + 1, col_index, 'Example' if header == 'Entry Number' else '', italic_format)
        main_sheet.set_column(col_index, col_index, width)

    start_row = header_row + 2

    # Add headers to the names sheet
    names_sheet.write('A1', 'Names')
    names_sheet.write('B1', 'Affiliations')

    # Populate the names sheet with data
    for row_index, name in enumerate(names, start=1):
        names_sheet.write(row_index, 0, name)
        for col_index, affiliation in enumerate(affiliations[name], start=1):
            names_sheet.write(row_index, col_index, affiliation)

    # Create helper columns for row indices
    for entry_idx in range(n_entries):
        for i in range(n_authors):
            name_col = list(col_layout.keys()).index(f'Author {i + 1}')
            helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
            affiliation_col = list(col_layout.keys()).index(f'Affiliation {i + 1}')
            # print("HMM", i + 1, helper_col, col_num_to_col_letter(helper_col))
            row_index_cell = f'{col_num_to_col_letter(helper_col + 1)}{start_row + entry_idx + 1}'

            # Set data validation for names
            main_sheet.data_validation(start_row + entry_idx, name_col, start_row + entry_idx, name_col, {
                'validate': 'list',
                'source': f'Names!$A$2:$A${len(names) + 1}',
                'input_message': 'Write the name in <first_name> <last_name> format\n\n' + \
                                  'If it is missing and you cannot find it in the "Names" sheet with ctrl+f (check for usage of . or - or middle names/abbreviations), enter it yourself and select "yes" to override data validation.',
                'error_type': 'warning'
            })

            # Populate helper column with row index
            main_sheet.write_formula(start_row + entry_idx, helper_col,
                                     f'MATCH({col_num_to_col_letter(name_col + 1)}{start_row + entry_idx + 1}, Names!$A$2:$A${len(names) + 1}, 0) + 1')

            # Set data validation for affiliations
            affiliation_tooltip = 'Select Affiliation from the list\n\n' + \
                                  'If not available, enter it yourself and override data validation.\n' + \
                                  'If there are multiple affiliations, add the same author multiple times.\n' + \
                                  'If the same affiliation appears more than once, just select any'

            # Add data validation for affiliations based on helper column
            affiliation_col = list(col_layout.keys()).index(f'Affiliation {i + 1}')
            helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')

            # Set data validation for affiliations
            main_sheet.data_validation(start_row + entry_idx, affiliation_col, start_row + entry_idx, affiliation_col, {
                'validate': 'list',
                'source': f'=INDIRECT("Names!B" & {row_index_cell} & ":I" & {row_index_cell})',
                'input_message': affiliation_tooltip,
                'error_type': 'warning'
            })

            # Set the tooltip based on the color
            publication_color = prefill_publications[entry_idx].get(f'Affiliation {i + 1}', [None, None, ''])[1]
            if publication_color in color_messages:
                tooltip_message = color_messages[publication_color]
            main_sheet.write_comment(start_row + entry_idx, affiliation_col, tooltip_message)

    # Add Entry Number column data
    entry_number_col = list(col_layout.keys()).index('Entry Number')
    for entry_idx in range(n_entries):
        main_sheet.write(start_row + entry_idx, entry_number_col, entry_idx + 1)

    # Prefill publications if provided
    if prefill_publications:
        for entry_idx, publication in enumerate(prefill_publications):
            for col_index, header in enumerate(col_layout.keys()):
                if header in publication:
                    cell_value = publication[header][0]
                    main_sheet.write(start_row + entry_idx, col_index, cell_value, cell_color_formats.get(publication[header][1], wrap_format))

    # Hide the helper columns
    # for i in range(n_authors):
    #     helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
    #     main_sheet.set_column(helper_col, helper_col, None, None, {'hidden': True})

    workbook.close()

# Helper function for column letter conversion
def col_num_to_col_letter(col_num):
    """Convert a column number (1-indexed) to a column letter."""
    letter = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter
