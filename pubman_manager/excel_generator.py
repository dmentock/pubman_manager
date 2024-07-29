import xlsxwriter
from collections import OrderedDict

def col_num_to_col_letter(col_num):
    """Convert a column number to a column letter (e.g., 1 -> A, 27 -> AA)."""
    div = col_num
    col_letter = ''
    while div > 0:
        (div, mod) = divmod(div - 1, 26)
        col_letter = chr(mod + 65) + col_letter
    return col_letter

def create_sheet(file_path, names_affiliations, column_details, n_authors, n_entries):
    col_layout = OrderedDict([('Entry Number', 15)])
    col_layout.update(column_details)

    for i in range(n_authors):
        col_layout[f'Name {i + 1}'] = 30
        col_layout[f'Affiliation {i + 1}'] = 35

    for i in range(n_authors):
        col_layout[f'Helper {i + 1}'] = 10  # Add helper columns at the end

    # Create a workbook and add worksheets
    workbook = xlsxwriter.Workbook(file_path)
    main_sheet = workbook.add_worksheet("MainSheet")
    names_sheet = workbook.add_worksheet("Names")

    # Create formats for the headers, italic text, and wrapped text
    header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter'})
    italic_format = workbook.add_format({'italic': True, 'text_wrap': True})

    wrap_format = workbook.add_format({'text_wrap': True})

    # Extract names and affiliations
    names = list(names_affiliations.keys())
    affiliations = names_affiliations

    # Write names in the rows above the header for autocomplete
    for row_index, name in enumerate(names):
        for col_index in range(n_authors):
            col = list(col_layout.keys()).index(f'Name {col_index + 1}')
            main_sheet.write(row_index, col, name)

    # Hide those rows
    for row_index in range(len(names)):
        main_sheet.set_row(row_index, None, None, {'hidden': True})
        col_index = 0

    start_row = len(names)

    for i, (header, width) in enumerate(col_layout.items()):
        main_sheet.write(start_row, col_index, header, header_format)
        main_sheet.write(start_row + 1, col_index, 'Example' if header == 'Entry Number' else '', italic_format)
        main_sheet.set_column(col_index, col_index, width)
        col_index += 1

    # Add headers to the names sheet
    names_sheet.write('A1', 'Names')
    names_sheet.write('B1', 'Affiliations')

    # Populate the names sheet with data
    for row_index, name in enumerate(names, start=1):
        names_sheet.write(row_index, 0, name)
        for col_index, affiliation in enumerate(affiliations[name], start=1):
            names_sheet.write(row_index, col_index, affiliation)

    # Create helper columns for row indices and affiliations

    for entry_idx in range(n_entries):
        for i in range(len(col_layout)):
            main_sheet.write(start_row + 2 + entry_idx, i, '', wrap_format)
        for i in range(n_authors):
            name_col = list(col_layout.keys()).index(f'Name {i + 1}')
            helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
            affiliation_col = list(col_layout.keys()).index(f'Affiliation {i + 1}')
            row_index_cell = col_num_to_col_letter(helper_col + 1) + str(start_row + 2 + entry_idx)

            # Set data validation for names
            main_sheet.data_validation(start_row + 1 + entry_idx, name_col, start_row + 1 + entry_idx, name_col, {
                'validate': 'list',
                'source': f'Names!$A$2:$A${len(names) + 1}',
                'input_message': f'Write the name in <first_name> <last_name> format or select from the list\n\n' + \
                                  'If it is missing and you cannot find it in the "Names" sheet with ctrl+f (check for usage of . or - or middle names/abbreviations), enter it yourself and override data validation.',
                'error_type': 'warning'
            })

            # Populate helper column with row index
            main_sheet.write_formula(start_row + 1 + entry_idx, helper_col,
                                     f'MATCH({col_num_to_col_letter(name_col + 1)}{start_row + 2 + entry_idx}, Names!$A$2:$A${len(names) + 1}, 0) + 1')

            # Set data validation for affiliations
            main_sheet.data_validation(start_row + 1 + entry_idx, affiliation_col, start_row + 1 + entry_idx, affiliation_col, {
                'validate': 'list',
                'source': f'=INDIRECT("Names!B" & {row_index_cell} & ":I" & {row_index_cell})',
                'input_message': f'Select Affiliation from the list\n\n' + \
                                  'If not available, enter it yourself and override data validation.\n' + \
                                  'If there are multiple affiliations, add the same author multiple times.\n' + \
                                  'If the same affiliation appears more than once, just select any',
                'error_type': 'warning'
            })

    # Add Entry Number column data
    entry_number_col = list(col_layout.keys()).index('Entry Number')
    for entry_idx in range(1, n_entries):
        main_sheet.write(start_row + 1 + entry_idx, entry_number_col, entry_idx)

    # Hide the helper columns
    for i in range(n_authors):
        helper_col = list(col_layout.keys()).index(f'Helper {i + 1}')
        main_sheet.set_column(helper_col, helper_col, None, None, {'hidden': True})

    workbook.close()