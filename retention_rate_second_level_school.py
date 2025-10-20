import pandas as pd
import requests
import io
import os
import time
from unittest.mock import patch, MagicMock
from utils import timed_call

# --- Configuration ---
DATA_URL = 'https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en.'
OUTPUT_FILE_NAME = 'processed_student_data'

def download_and_load_data(url: str) -> pd.DataFrame:
    """
    Downloads the CSV data from the specified URL and loads it into a pandas DataFrame.
    Args:
        url: The URL to download the CSV data from.
    Returns: A pandas DataFrame containing the raw data, or None if the download fails.
    """
    print("\n-----------------Starting data ingestion---------------------\n")
    print(f"\nDownloading data from : {url}")
    try:
        # Use requests to get the content
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Raises HTTP Error for bad responses

        # Use io.StringIO to treat the text content as a file
        data_io = io.StringIO(response.text)

        # # Define schema for columns (to ensure correct data types)
        # schema = {
        #     'statistic"': str,
        #     'statistic_label': str,
        #     'c02351v02955': str,
        #     'type_of_school': str,
        #     'c02199v02655': str,
        #     'sex': str,
        #     'tlist_a1': str,
        #     'year': int,
        #     'unit': str,
        #     'value': float
        # }
        # df = pd.read_csv(data_io, dtype=schema)
        
        # Read the data, assuming the first row contains headers
        df = pd.read_csv(data_io, encoding="utf-8-sig")
        # Fix column name if BOM or quotes present The column name 'ï»¿"statistic"' contains the characters ï»¿ because the CSV file starts with a Byte Order Mark (BOM), which is common in UTF-8 encoded files created by some spreadsheet programs (like Excel).
        df.rename(columns={df.columns[0]: 'statistic'}, inplace=True)

        print(f"Data downloaded successfully. Total records: {len(df)}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during loading: {e}")
        return None


def transform_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the raw DataFrame to filter, group, aggregate, and rename columns.
    Args:
         df: The raw pandas DataFrame.
    Returns: The processed and aggregated DataFrame.
    """
    print("\n-----------------Starting data transformation and aggregation---------------------\n")
    # Strip whitespace from column names, then convert to lower case
    df.columns = df.columns.str.strip().str.lower()

    #Filter only rows where 'statistic label' contains "First Year"
    df_filtered = df[df['statistic label'].str.contains('First Year', case=False, na=False)].copy()

    # Select only records where the UNIT is number and exclude %
    df_filtered = df_filtered[df_filtered['unit'].str.lower() == 'number'].copy()
    print(f"Records after filtering for UNIT == 'number': {len(df_filtered)}")

    # Check if any data remains after filtering
    if df_filtered.empty:
        print("Warning: No records found after filtering for 'First Year' in Statistic Label column.")
        return pd.DataFrame()

    # Ensure 'year' and 'value' are numeric for calculation
    # Coercing errors to NaN and dropping those rows for clean aggregation
    df_filtered['year'] = pd.to_numeric(df_filtered['year'], errors='coerce')
    df_filtered['value'] = pd.to_numeric(df_filtered['value'], errors='coerce')
    df_filtered.dropna(subset=['year', 'value'], inplace=True)
    print(f"Records after cleaning 'year' and 'value': {len(df_filtered)}\n")
    
    # Calculate: Create 5-by-5 year period groupings
    min_year = int(df_filtered['year'].min())
    print(f"Minimum year in data: {min_year}")

    max_year = int(df_filtered['year'].max())
    print(f"Maximum year in data: {max_year}")

    # Calculate the number of 5-year steps needed
    num_years = max_year - min_year
    num_periods = (num_years // 5) + 1 #number of 5-year periods

    # Determine the stopping point for the bins
    stop_point = min_year + (num_periods + 1) * 5
    
    # Create bins for 5-year periods
    bins = range(min_year, stop_point, 5)
    
    # The labels represent the start year to start year + 4
    labels = [f"{i}-{i+4}" for i in bins[:-1]]

    print(f"Year bins:{list(bins)}")
    print(f"Year labels: {labels}\n")

    # Use pd.cut to assign each year to its corresponding 5-year period
    df_filtered['five_year_period'] = pd.cut(
        df_filtered['year'], 
        bins=bins, 
        labels=labels, 
        right=False, #for left-inclusive bins
        include_lowest=True
    ).astype(str)

    # print(f"Records after assigning five-year periods:\n {df_filtered[['year', 'five_year_period']].head()}")


    # The lowest bin should always be the label that starts with min_year.
    lowest_label = f"{min_year}-{min_year+4}"
    df_filtered.loc[df_filtered['year'] == min_year, 'five_year_period'] = lowest_label
    # print(f"Records after adjusting lowest five-year period: \n {df_filtered[['year', 'five_year_period']].head()}")
    

    # Aggregate: Group by the new period and 'sex', and sum the 'value' (number of students)
    df_aggregated = df_filtered.groupby(['five_year_period', 'sex'])['value'].sum().reset_index()

    # Rename: Clean up the final column names
    df_aggregated.rename(columns={'value': 'retention_count', 'sex': 'sex_category'}, inplace=True)

    # This ensures the output is a whole number while correctly handling NaNs if they occurred during aggregation.
    df_aggregated['retention_count'] = df_aggregated['retention_count'].astype('Int64')
    print(f"Records after aggregation:\n\n {df_aggregated.head()}")

    print(f"\nData transformed and aggregated.")
    return df_aggregated


def save_data(df: pd.DataFrame, base_name: str):
    """
    Writes the processed DataFrame to CSV and Parquet formats.
    Args:
        df: The processed pandas DataFrame.
        base_name: The base name for the output files
    """
    print("\n--------------------Saving processed data to files ---------------------\n")   
    if df.empty:
        print("Cannot save empty DataFrame.")
        return

    # Ensure the 'transformed' directory exists
    output_dir = "transformed"
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    parquet_path = os.path.join(output_dir, f"{base_name}.parquet")
    
    # Save to CSV
    try:
        df.to_csv(csv_path, index=False)
        print(f"Data successfully saved to CSV: {os.path.abspath(csv_path)}")
    except Exception as e:
        print("Error saving to CSV: {e}")

    # Save to Parquet
    try:
        df.to_parquet(parquet_path, index=False)
        print(f"Data successfully saved to Parquet: {os.path.abspath(parquet_path)}")
    except Exception as e:
        print(f"Error saving to Parquet: {e}")

def main():
    """
    Main orchestration function.
    """
    # 1. Download and Load the data from the public URL
    raw_df = timed_call(download_and_load_data, DATA_URL)

    if raw_df is None or raw_df.empty:
        print("Pipeline failed due to inability to load data.")
        return

    # 2. Transform and Aggregate
    processed_df = timed_call(transform_and_aggregate, raw_df)

    # 3. Save Output of first year student counts by sex and 5year period
    timed_call(save_data, processed_df, OUTPUT_FILE_NAME)

    # 4. Verification Step: Load and print first few rows of the saved Parquet file
    print("\n-----------------Verification Step: Loading saved Parquet file---------------------\n")
    try:
        loaded_df = timed_call(pd.read_parquet, os.path.join("transformed", f"{OUTPUT_FILE_NAME}.parquet"))
        print("Verification: Loaded data from Parquet file:")
        print(loaded_df.head())
    except Exception as e:
        print(f"Error during verification step: {e}")

if __name__ == '__main__':
    main()
