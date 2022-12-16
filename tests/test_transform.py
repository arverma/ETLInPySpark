__author__ = "aman.rv"

from src.transform import transform


def test_transform(spark_session):
    df_dict = {
        "fact": spark_session.read.csv(
            "tests/resources/input/orders_data.csv", header=True, inferSchema=True
        ),
        "lookup": spark_session.read.csv(
            "tests/resources/input/citytier_pincode.csv", header=True, inferSchema=True
        ),
    }
    df_actual = transform(df_dict)
    expected_df = spark_session.read.csv(
        "tests/resources/expected/*", header=True, inferSchema=True
    )

    assert sorted(expected_df.collect()) == sorted(df_actual.collect())
