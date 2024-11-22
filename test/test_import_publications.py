import pytest
import pandas as pd
from unittest.mock import patch
from pubman_manager import PubmanBase
from pathlib import Path

@pytest.fixture
def mock_api():
    return PubmanBase("dummy_token", "dummy_pw", base_url="http://dummy_url")

@patch.object(PubmanBase, 'create_event_publication')
def test_process_excel_and_create_publications(mock_create_event_publication, mock_api):
    file_path = Path(__file__).parent / 'test_import.xlsx'

    mock_api.process_excel_and_create_publications(file_path)

    # Print the actual calls made to create_event_publication for debugging
    for call in mock_create_event_publication.call_args_list:
        print("Actual call:", call)

    # # Define expected arguments for the first call
    # expected_call_1 = (
    #     'deRSE23 - Conference for Research Software Engineering in Germany',
    #     '20.03.2023',
    #     '22.03.2023',
    #     '21.03.2023',
    #     'Paderborn, Germany',
    #     'n',
    #     'DAMASK: Challenges in collaborative development and outlook',
    #     {
    #         'Daniel Otto de Mentock': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society',
    #         'Sharan Roongta': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society',
    #         'Philip Eisenlohr': 'Michigan State University, Chemical Engineering and Materials Science, East Lansing, MI 48824, USA',
    #         'Martin Diehl': 'Department of Computer Science, KU Leuven, Celestijnenlaan 200 A, Leuven 3001, Belgium',
    #         'Franz Roters': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society'
    #     }
    # )

    # # Define expected arguments for the second call
    # expected_call_2 = (
    #     'TMS - Algorithm Development in Materials Science and Engineering',
    #     '03.03.2024',
    #     '07.03.2024',
    #     '07.03.2024',
    #     'Orlando, USA',
    #     'n',
    #     'Challenges of Developing and Scaling up DAMASK, a Unified Large-strain Multi-physics Crystal Plasticity Simulation Software',
    #     {
    #         'Daniel Otto de Mentock': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society',
    #         'Sharan Roongta': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society',
    #         'Pratheek Shanthraj': 'United Kingdom Atomic Energy Authority, Culham, Abingdon, UK',
    #         'Philip Eisenlohr': 'Michigan State University, Chemical Engineering and Materials Science, East Lansing, MI 48824, USA',
    #         'Martin Diehl': 'Department of Computer Science, KU Leuven, Celestijnenlaan 200 A, Leuven 3001, Belgium',
    #         'Franz Roters': 'Theory and Simulation, Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society'
    #     }
    # )

    # # Print expected calls for comparison
    # print("Expected call 1:", expected_call_1)
    # print("Expected call 2:", expected_call_2)

    # Perform the assertions
    # mock_create_event_publication.assert_any_call(*expected_call_1)
    # mock_create_event_publication.assert_any_call(*expected_call_2)

if __name__ == "__main__":
    pytest.main(["-v"])
