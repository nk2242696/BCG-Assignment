from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def extract_data(spark, config):
    """
    :param spark:
    :param config:
    :return: Raw dataframe after reading Charges file type
    """

    raw_df = spark \
        .read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("schema", _get_schema()) \
        .load(f"{config.get('source_data_path')}/Charges_use.csv")

    return raw_df


def _get_schema():
    """
    schema for Charges file type
    :return: dict
    """
    schema = StructType([
        StructField("CRASH_ID", StringType(), False),
        StructField("UNIT_NBR", IntegerType(), False),
        StructField("PRSN_NBR", IntegerType(), False),
        StructField("CHARGE", StringType(), True),
        StructField("CITATION_NBR", StringType(), True)
    ])
    return schema


