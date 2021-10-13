from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def extract_data(spark, config):
    """
    :param spark:
    :param config:
    :return: Raw dataframe after reading Endorse file type
    """

    raw_df = spark \
        .read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("schema", _get_schema()) \
        .load(f"{config.get('source_data_path')}/Primary_Person_use.csv")

    return raw_df


def _get_schema():
    """
        schema for Primary Person file type
        :return: dict
    """
    schema = StructType([
        StructField("CRASH_ID", IntegerType(), False),
        StructField("UNIT_NBR", IntegerType(), False),
        StructField("PRSN_NBR", IntegerType(), False),
        StructField("PRSN_TYPE_ID", StringType(), True),
        StructField("PRSN_OCCPNT_POS_ID", StringType(), True),
        StructField("PRSN_INJRY_SEV_ID", StringType(), True),
        StructField("PRSN_AGE", IntegerType(), True),
        StructField("PRSN_ETHNICITY_ID", StringType(), True),
        StructField("PRSN_GNDR_ID", StringType(), True),
        StructField("PRSN_EJCT_ID", StringType(), True),
        StructField("PRSN_REST_ID", StringType(), True),
        StructField("PRSN_AIRBAG_ID", StringType(), True),
        StructField("PRSN_HELMET_ID", StringType(), True),
        StructField("PRSN_SOL_FL", StringType(), True),
        StructField("PRSN_ALC_SPEC_TYPE_ID", StringType(), True),
        StructField("PRSN_ALC_RSLT_ID", StringType(), True),
        StructField("PRSN_BAC_TEST_RSLT", DoubleType(), True),
        StructField("PRSN_DRG_SPEC_TYPE_ID", StringType(), True),
        StructField("PRSN_DRG_RSLT_ID", StringType(), True),
        StructField("DRVR_DRG_CAT_1_ID", StringType(), True),
        StructField("PRSN_DEATH_TIME", StringType(), True),
        StructField("INCAP_INJRY_CNT", IntegerType(), True),
        StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
        StructField("POSS_INJRY_CNT", IntegerType(), True),
        StructField("NON_INJRY_CNT", IntegerType(), True),
        StructField("UNKN_INJRY_CNT", IntegerType(), True),
        StructField("TOT_INJRY_CNT", IntegerType(), True),
        StructField("DEATH_CNT", IntegerType(), True),
        StructField("DRVR_LIC_TYPE_ID", StringType(), True),
        StructField("DRVR_LIC_STATE_ID", StringType(), True),
        StructField("DRVR_LIC_CLS_ID", StringType(), True),
        StructField("DRVR_ZIP", IntegerType(), True),
    ])
    return schema


