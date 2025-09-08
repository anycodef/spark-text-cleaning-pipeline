import pytest
import os
import subprocess
import pandas as pd
import pyarrow.parquet as pq
from unittest.mock import patch
import sys
from pipeline_cleaning import main as pipeline_main
import glob

INPUT_FILE = "Datos_Caso6.xlsx"
BASE_OUTPUT_NAME = "Datos_Caso6-clean"

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    """
    Remove output files before and after tests.
    """
    files_to_clean = [
        f"{BASE_OUTPUT_NAME}.csv",
        f"{BASE_OUTPUT_NAME}.xlsx",
        f"{BASE_OUTPUT_NAME}.parquet"
    ]

    for f in files_to_clean:
        if os.path.exists(f):
            if os.path.isdir(f):
                import shutil
                shutil.rmtree(f)
            else:
                os.remove(f)

    yield

    for f in files_to_clean:
        if os.path.exists(f):
            if os.path.isdir(f):
                import shutil
                shutil.rmtree(f)
            else:
                os.remove(f)

def run_pipeline(export_flags):
    """
    Helper function to run the pipeline script's main function directly,
    allowing mocks to work.
    """
    test_argv = ["pipeline_cleaning.py", INPUT_FILE] + export_flags
    with patch.object(sys, 'argv', test_argv):
        with patch('pipeline_cleaning.translator') as mock_translator:
            mock_translator.translate.return_value = "translated"
            pipeline_main()

def get_input_df():
    """Reads the input excel file using the same logic as the script."""
    df = pd.read_excel(INPUT_FILE, engine='openpyxl')
    if len(df.columns) == 1 and ',' in df.columns[0]:
        from io import StringIO
        csv_header = df.columns[0]
        csv_data = "\n".join(df.iloc[:, 0].astype(str).tolist())
        full_csv_string = csv_header + "\n" + csv_data
        df = pd.read_csv(StringIO(full_csv_string))
    return df

def test_export_to_csv():
    """Tests the --export-to-csv flag."""
    output_dir = f"{BASE_OUTPUT_NAME}.csv"
    run_pipeline(["--export-to-csv"])

    assert os.path.isdir(output_dir)
    # Find the part file(s) created by Spark
    part_files = glob.glob(os.path.join(output_dir, 'part-*.csv'))
    assert len(part_files) > 0

    df_input = get_input_df()
    # Read the partitioned csv files
    df_output = pd.read_csv(part_files[0])

    assert len(df_output) > 0
    assert "text_clean" in df_output.columns
    assert df_output["text_clean"].notna().all()

def test_export_to_parquet():
    """Tests the --export-to-parquet flag."""
    output_dir = f"{BASE_OUTPUT_NAME}.parquet"
    run_pipeline(["--export-to-parquet"])

    assert os.path.exists(output_dir)
    df_input = get_input_df()
    df_output = pq.read_table(output_dir).to_pandas()

    assert len(df_output) == len(df_input)
    assert "text_clean" in df_output.columns
    assert df_output["text_clean"].notna().all()

def test_export_csv_and_parquet():
    """Tests exporting to csv and parquet formats at once."""
    csv_dir = f"{BASE_OUTPUT_NAME}.csv"
    parquet_dir = f"{BASE_OUTPUT_NAME}.parquet"

    run_pipeline(["--export-to-csv", "--export-to-parquet"])

    assert os.path.isdir(csv_dir)
    assert os.path.exists(parquet_dir)
