__author__ = 'aman.rv'

import argparse
import simplejson


def extract_source_data(spark, sources):
    """

    :param sources:
    :param spark:
    :return:
    """
    df_dict = {}
    for source in sources:
        df_dict[source["name"]] = read_source_data(spark, source["storeType"], source["storeConfig"])
    return df_dict


def read_source_data(spark, source_type, source_config):
    if source_type == "GCS":
        return spark.read.option("format", source_config["format"]).load(source_config["path"], header=True)


def save_to_sink(df, config):
    """

    :param spark:
    :param df:
    :param config:
    :return:
    """
    sink_conf = config["storeConfig"]
    if config["storeType"] == "GCS":
        df.write.option("mode", sink_conf["mode"]).option("format", sink_conf["format"]).save(
            sink_conf["path"]
        )


def read_job_config():
    """

    :return:
    """
    args = parse_known_cmd_args()
    with open(args.config_file_name) as config_file:
        try:
            return simplejson.load(config_file)
        except simplejson.errors.JSONDecodeError as error:
            raise simplejson.errors.JSONDecodeError(
                f"Issue with the Job Config: {args.config_file_name}.json {error}"
            )


def parse_known_cmd_args():
    """
    Parse Cmd known args
    Return Example: Namespace(config_file_name=job_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_file_name", help="specify config file name", action="store"
    )
    return parser.parse_known_args()[0]