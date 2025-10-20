import pytest
import pandas as pd
import os
import requests
import shutil
import sys 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from retention_rate_second_level_school import transform_and_aggregate, save_data, download_and_load_data
from unittest.mock import patch, MagicMock

# --- Configuration for Tests ---
MOCK_URL = "http://mock.cso.ie/data.csv"
OUTPUT_FILE_NAME = 'processed_student_data'
OUTPUT_DIR = "transformed"
CSV_PATH = os.path.join(OUTPUT_DIR, f'{OUTPUT_FILE_NAME}.csv')
PARQUET_PATH = os.path.join(OUTPUT_DIR, f'{OUTPUT_FILE_NAME}.parquet')


@pytest.fixture
def mock_raw_df():
    """
    Pytest fixture providing a mock DataFrame reflecting the raw CSV structure
    and data needed for the 'First Year' filtering and 5-year aggregation.
    """
    return pd.DataFrame({
        # NOTE: This column is renamed to 'statistic' inside download_and_load_data
        'STATISTIC': ['EDA14C1', 'EDA14C1', 'EDA14C1', 'EDA14C1', 'EDA14C1', 'EDA14C1', 'EDA14C1', 'Total'], 
        'Statistic Label': [
            'Entrants to First Year of Junior Cycle', 
            'Entrants to First Year of Junior Cycle', 
            'Entrants to First Year of Junior Cycle',
            'Entrants to First Year of Junior Cycle',
            'Entrants to First Year of Junior Cycle',
            'Entrants to First Year of Junior Cycle',
            'Entrants to First Year of Junior Cycle',
            'Total Students Enrolled' # Will be filtered out
        ],
        'Year': [2010, 2011, 2014, 2015, 2019, 2020, 2024, 2025],
        'Sex': ['Male', 'Female', 'Male', 'Female', 'Male', 'Female', 'Both sexes', 'Male'],
        'VALUE': [100.0, 50.0, 150.0, 250.0, 300.0, 400.0, 500.0, 999.0],
        'UNIT': ['Number'] * 8
    })

@pytest.fixture(scope="session", autouse=True)
def cleanup_files():
    """
    Fixture for cleaning up the 'transformed' directory after all tests are run.
    """
    # Setup: runs before tests
    yield 
    # Teardown: runs after tests
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)

# --- Test Functions ---

def test_filtering_and_lowercasing(mock_raw_df):
    """
    Tests that only 'First Year' records are kept, columns are lowercased, 
    and the correct filtering column ('statistic label') is used.
    """
    # NOTE: The df passed here does NOT have the column 0 renamed yet, 
    # but the subsequent strip/lower operation makes 'statistic label' accessible.
    
    result_df = transform_and_aggregate(mock_raw_df.copy())
    
    # Assertions for final output structure
    expected_cols = ['five_year_period', 'sex_category', 'retention_count']
    assert list(result_df.columns) == expected_cols
    assert len(result_df) == 6, "Expected 6 aggregated rows after filtering and grouping."

def test_grouping_and_summation_with_right_false(mock_raw_df):
    """
    Tests the 5-year grouping and the correctness of the total sum using the new binning logic 
    """
    df_result = transform_and_aggregate(mock_raw_df.copy())
    
    # Total sum of 'First Year' retention counts should equal the sum of the relevant 'VALUE's
    expected_total = 1750.0
    calculated_total = df_result['retention_count'].sum()

    assert calculated_total == pytest.approx(expected_total)

    # Test specific aggregated group values based on the new bins:
    
    # 2010-2014 Period: Includes 2010, 2011, 2014
    # Male: 100 (2010) + 150 (2014) = 250.0
    g1_m = df_result[
        (df_result['five_year_period'] == '2010-2014') & 
        (df_result['sex_category'] == 'Male')
    ]['retention_count'].iloc[0]
    assert g1_m == pytest.approx(250.0)

    # 2020-2024 Period: Includes 2020, 2024
    # Female: 400 (2020) = 400.0
    g3_f = df_result[
        (df_result['five_year_period'] == '2020-2024') & 
        (df_result['sex_category'] == 'Female')
    ]['retention_count'].iloc[0]
    assert g3_f == pytest.approx(400.0)

def test_save_data():
    """
    Tests that save_data successfully creates the necessary directory and files.
    """
    # Use 'test_output' temporarily to avoid conflict with main script run
    mock_base_name = 'test_output'
    mock_csv_path = os.path.join(OUTPUT_DIR, f'{mock_base_name}.csv')
    mock_parquet_path = os.path.join(OUTPUT_DIR, f'{mock_base_name}.parquet')

    mock_df = pd.DataFrame({'Period': ['A', 'B'], 'Count': [10, 20]})
    save_data(mock_df, mock_base_name)

    # Assertions for file existence in the correct subdirectory
    assert os.path.exists(OUTPUT_DIR)
    assert os.path.exists(mock_csv_path)
    assert os.path.exists(mock_parquet_path)

    # Verify CSV content
    df_csv = pd.read_csv(mock_csv_path)
    assert df_csv.equals(mock_df)

    # Verify Parquet content
    df_parquet = pd.read_parquet(mock_parquet_path)
    assert df_parquet.equals(mock_df)


@patch('retention_rate_second_level_school.requests.get')
def test_download_success_with_bom_fix(mock_get):
    """
    Tests successful data download and DataFrame creation, ensuring the BOM fix (column rename) works.
    """
    # 1. Configure the mock response object
    mock_response = MagicMock()
    mock_response.status_code = 200
    
    # Simulate a CSV where the first column header has a BOM/quote issue (handled by pd.read_csv + rename)
    # The actual rename occurs after read_csv, targeting df.columns[0]
    csv_content = 'STATISTIC,"Statistic Label",Year,VALUE\nEDA14C1,"First Year",2020,1000\nTotal,"Total Students",2021,2000'
    mock_response.text = csv_content
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # 2. Call the function
    df = download_and_load_data(MOCK_URL)
    
    # 3. Pytest assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    # Verify the first column was successfully renamed to 'statistic'
    assert 'statistic' in df.columns 
    assert df['statistic'].iloc[0] == 'EDA14C1'
    assert df['VALUE'].iloc[0] == 1000 # Verify value integrity

@patch('retention_rate_second_level_school.requests.get')
def test_download_failure(mock_get):
    """
    Tests failure scenario when the HTTP request returns an error code.
    """
    # Configure the mock response for a 404 error
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('404 Not Found')
    mock_get.return_value = mock_response

    # Call the function
    df = download_and_load_data(MOCK_URL)
    
    # Pytest assertion
    assert df is None
