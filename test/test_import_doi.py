import re
import copy


# Testing the improved function
names_affiliations = [
    'Leonardo Shoji Aota',
    'Leonardo Aota',
    'Taylor Swift',
    'Bob Marley',
    'John Michael Smith',
    'John Smith'
]

test_names = [
    'L. S. Aota',
    'L. Aota',
    'J. M. Smith',
    'J. Smith',
    'T. Swift',
    'B. Marley',
    'Bob John Marley',
    'Bob J. Marley'
]

# Test the function with different abbreviated names
matched_names = {abbreviated_name: process_name(names_affiliations, abbreviated_name) for abbreviated_name in test_names}
print(matched_names)
