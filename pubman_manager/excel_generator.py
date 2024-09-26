import xlsxwriter
from collections import OrderedDict

def create_sheet(file_path, names_affiliations, column_details, n_authors, prefill_publications=None, n_entries=None, save=True):
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

    header_format = workbook.add_format({'text_wrap': True, 'bold': True, 'align': 'center', 'valign': 'vcenter', 'locked': True})
    italic_format = workbook.add_format({'italic': True, 'text_wrap': True, 'locked': True})
    wrap_format = workbook.add_format({'text_wrap': True, 'locked': True})

    disclaimer_format = workbook.add_format({
        'bold': True,
        'font_color': 'red',
        'text_wrap': True,
        'valign': 'vcenter'
    })

    colors = {
        'gray': '#d3d3d3',
        'yellow': '#ffffe0',
        'orange': '#ffd8b1',
        'red': '#ff9999',
        'purple': '#e6e6fa',
        'pink': '#FFEEEE'
    }

    color_messages = {
        'gray': 'Author or similar Affiliation not found in database -> using affiliation from publisher (err={err})',
        'yellow': 'Author found in database -> using most similar affiliation to the one provided by the publisher (err={err})',
        'orange': 'Author found in database, but no affiliation provided by publisher -> guessing affiliation based on publication title (err={err})',
        'red': 'Author not found in database, no affiliation provided by publisher -> guessing based on other author affiliations (needs revision)',
        'purple': 'Generic Max Planck Affiliation was provided by publisher, but the precise group was not found in the database (err={err})',
        'pink': 'Author or similar Affiliation not found in database -> using most likely affiliation from other authors (err={err})'
    }

    cell_color_formats = {color: workbook.add_format({'bg_color': colors[color], 'text_wrap': True}) for color in colors.keys()}

    names = list(names_affiliations.keys())
    affiliations = names_affiliations

    for row_index, name in enumerate(names):
        for col_index in range(n_authors):
            col = list(col_layout.keys()).index(f'Author {col_index + 1}')
            main_sheet.write(row_index, col, name)

    for row_index in range(len(names)):
        main_sheet.set_row(row_index, None, None, {'hidden': True})


    disclaimer_lines = [
        "Please try to avoid copy-pasting in areas with dropdowns, as this will break the underlying data validation and make subsequent editing difficult.",
        "Only do so if you are sure you won't have to change the author name or affiliations afterwards, e.g. when pasting from identical sections in previous entries."
    ]

    disclaimer_start_row = len(names) + 1
    for i, line in enumerate(disclaimer_lines):
        main_sheet.merge_range(
            f'A{disclaimer_start_row + i}:{col_num_to_col_letter(len(col_layout))}{disclaimer_start_row + i}',
            line, disclaimer_format
        )
    header_row = disclaimer_start_row + len(disclaimer_lines)
    for col_index, (header, (width, comment)) in enumerate(col_layout.items()):
        main_sheet.write(header_row, col_index, header, header_format)
        main_sheet.write(header_row + 1, col_index, 'Example' if header == 'Entry Number' else '', italic_format)
        main_sheet.set_column(col_index, col_index, width)

    start_row = header_row + len(disclaimer_lines) - 1
    names_sheet.write('A1', 'Names')
    names_sheet.write('B1', 'Affiliations')
    for row_index, name in enumerate(names, start=1):
        names_sheet.write(row_index, 0, name)
        for col_index, affiliation in enumerate(affiliations[name], start=1):
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
            affiliation_tooltip = ''
            if prefill_publications and entry_idx > 0:
                publication_color = prefill_publications[entry_idx-1].get(f'Affiliation {i + 1}', [None, None, ''])[1]
                compare_error = prefill_publications[entry_idx-1].get(f'Affiliation {i + 1}', [None, None, '', ''])[3]
                if publication_color in color_messages:
                    affiliation_tooltip = color_messages[publication_color].format(err=compare_error)
                main_sheet.write_comment(start_row + entry_idx, affiliation_col, affiliation_tooltip)

    entry_number_col = list(col_layout.keys()).index('Entry Number')
    for entry_idx in range(n_entries):
        if entry_idx > 0:
            main_sheet.write(start_row + entry_idx, entry_number_col, entry_idx)

    if prefill_publications:
        for entry_idx, publication in enumerate(prefill_publications):
            for col_index, header in enumerate(col_layout.keys()):
                if header in publication:
                    if publication[header]:
                        if not (cell_value := publication[header][0]):
                            cell_value = ''
                        main_sheet.write(start_row + 1 + entry_idx, col_index, cell_value, cell_color_formats.get(publication[header][1], wrap_format))
    else:
        for entry_idx in range(n_entries):
            for col_index, (header, (width, comment)) in enumerate(col_layout.items()):
                if not 'Helper' in header and header not in ['Entry Number']:
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
