__author__ = "aman.rv"

from unittest.mock import patch
from src.utils import extract_source_data


def test_extract_source_data(spark_session):
    # Frame input parameter to function to be tested
    sources = [
        {
            "name": "fact",
            "storeType": "GCS",
            "storeConfig": {
                "path": "tests/resources/input/orders_data.csv",
                "format": "csv",
            },
        },
        {
            "name": "lookup",
            "storeType": "GCS",
            "storeConfig": {
                "path": "tests/resources/input/citytier_pincode.csv",
                "format": "csv",
            },
        },
    ]

    # Frame the mock data to be returned by the patched function
    mock_df = spark_session.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])

    # Frame the expected data
    expected_dict = {"fact": mock_df, "lookup": mock_df}

    # Path any function/API call being made inside the function being tested
    with patch("src.utils.read_source_data", spec=True) as mock_read_source_data:
        mock_read_source_data.return_value = mock_df
        actual_dict = extract_source_data(spark_session, sources)

    # checking if the dataframe returned from the function is same as expected
    assert sorted(expected_dict["fact"].collect()) == sorted(
        actual_dict["fact"].collect()
    ) and sorted(expected_dict["lookup"].collect()) == sorted(
        actual_dict["lookup"].collect()
    )
