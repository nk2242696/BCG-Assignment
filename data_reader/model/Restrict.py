from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def extract_data(spark, config):
    """
    :param spark:
    :param config:
    :return: Raw dataframe after reading Restrict file type
    """

    raw_df = spark \
        .read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("schema", _get_schema()) \
        .load(f"{config.get('source_data_path')}/Restrict_use.csv")

    return raw_df


def _get_schema():
    """
        schema for Primary Restrict file type
        :return: dict
    """
    schema = StructType([
        StructField("CRASH_ID", IntegerType(), False),
        StructField("UNIT_NBR", IntegerType(), False),
        StructField("DRVR_LIC_RESTRIC_ID", StringType(), True)
    ])
    return schema


