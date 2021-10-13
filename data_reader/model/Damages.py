from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def extract_data(spark, config):
    """
    :param spark:
    :param config:
    :return: Raw dataframe after reading Damages file type
    """

    raw_df = spark \
        .read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("schema", _get_schema()) \
        .load(f"{config.get('source_data_path')}/Damages_use.csv")

    return raw_df


def _get_schema():
    """
    schema for Damages file type
    :return: dict
    """
    schema = StructType([
        StructField("CRASH_ID", IntegerType(), False),
        StructField("DAMAGED_PROPERTY", StringType(), True)
    ])
    return schema


