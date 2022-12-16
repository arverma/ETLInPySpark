__author__ = "aman.rv"

from pyspark.sql import SparkSession
from src.transform import transform
from src.utils import (
    extract_source_data,
    save_to_sink,
    read_job_config,
    parse_known_cmd_args,
)

if __name__ == "__main__":
    # Initialise Spark Session
    spark = SparkSession.builder.appName("ETLInPySpark").getOrCreate()
    args = parse_known_cmd_args()
    config = read_job_config(args)

    # Extract
    df_dict = extract_source_data(spark, config["source"])

    # Transform
    df_transformed = transform(df_dict)

    # Load
    save_to_sink(df_transformed, config["sink"], args.env)
