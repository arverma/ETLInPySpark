__author__ = "aman.rv"

from src.transform import transform


def test_transform(spark_session):
    # Frame input parameter to function to be tested
    df_dict = {
        "fact": spark_session.read.csv(
            "tests/resources/input/orders_data.csv", header=True, inferSchema=True
        ),
        "lookup": spark_session.read.csv(
            "tests/resources/input/citytier_pincode.csv", header=True, inferSchema=True
        ),
    }

    # Call the actual function that we want to test
    df_actual = transform(df_dict)

    # Reading the expected data
    expected_df = spark_session.read.csv(
        "tests/resources/expected/*", header=True, inferSchema=True
    )

    # checking if the dataframe returned from the function is same as expected
    assert sorted(expected_df.collect()) == sorted(df_actual.collect())
