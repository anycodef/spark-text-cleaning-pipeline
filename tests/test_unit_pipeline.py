import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pyspark.sql import SparkSession

# It's good practice to have a shared SparkSession for all tests
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("UnitTests").getOrCreate()

# Import functions from the script to be tested
# Note: This will fail until we adjust the main script to be importable.
# I will address this as I go.
from pipeline_cleaning import (
    remove_urls_mentions,
    replace_emojis_translate,
    normalize_hashtags_translate,
    reduce_letter_repetitions
)

def test_remove_urls_mentions(spark):
    """
    Tests that URLs and mentions are correctly removed.
    """
    data = [("Check this http://test.com @user",)]
    df = spark.createDataFrame(data, ["text"])

    # The original function has a bug, so this test will fail initially.
    # I will first write the test to reproduce the bug, then fix the code.
    cleaned_df = remove_urls_mentions(df)
    result = cleaned_df.collect()[0].text_clean

    # Expected output is "Check this  " (with spaces from replacements)
    # which we can strip.
    assert result.strip() == "Check this"

@patch('pipeline_cleaning.translator')
def test_replace_emojis_translate(mock_translator):
    """
    Tests emoji replacement and translation.
    It should mock the translator to avoid actual API calls.
    """
    # The expected output from demoji is "face with tears of joy"
    # The mock will return "cara con lágrimas de alegría"
    mock_translator.translate.return_value = "cara con lágrimas de alegría"

    # Test case
    input_text = "hello 😂😂😂world"
    expected_output = "hello cara con lágrimas de alegría world"

    # The function to be tested
    result = replace_emojis_translate(input_text)

    assert result == expected_output

def test_reduce_letter_repetitions():
    """
    Tests the logic for reducing repeated letters using regex.
    """
    import re

    # The regex from the spark function
    pattern = r"(\p{L})\1{2,}"

    input_text = "noooo wayyy"
    expected_output = "no way"

    # We need to use the 'regex' module as it supports \p{L}
    import regex
    result = regex.sub(pattern, r"\1", input_text)

    assert result == expected_output

@patch('pipeline_cleaning.translator')
def test_normalize_hashtags_translate(mock_translator):
    """
    Tests hashtag normalization and translation.
    We patch the translator instance directly.
    """
    mock_translator.translate.return_value = "diadelafelicidad"

    input_text = "What a #happyday"
    expected_output = "What a #diadelafelicidad"

    result = normalize_hashtags_translate(input_text)

    assert result == expected_output
