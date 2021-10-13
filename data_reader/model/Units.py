from pyspark.sql.types import StringType, StructField, StructType, IntegerType


def extract_data(spark, config):
    """
    :param spark:
    :param config:
    :return: Raw dataframe after reading Units file type
    """

    raw_df = spark \
        .read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("schema", _get_schema()) \
        .load(f"{config.get('source_data_path')}/Units_use.csv")

    return raw_df


def _get_schema():
    """
        schema for Units file type
        :return: dict
    """
    schema = StructType([
        StructField("CRASH_ID", IntegerType(), False),
        StructField("UNIT_NBR", IntegerType(), False),
        StructField("UNIT_DESC_ID", StringType(), False),
        StructField("VEH_PARKED_FL", StringType(), True),
        StructField("VEH_HNR_FL", StringType(), True),
        StructField("VEH_LIC_STATE_ID", StringType(), True),
        StructField("VIN", StringType(), True),
        StructField("VEH_MOD_YEAR", IntegerType(), True),
        StructField("VEH_COLOR_ID", StringType(), True),
        StructField("VEH_MAKE_ID", StringType(), True),
        StructField("VEH_MOD_ID", StringType(), True),
        StructField("VEH_BODY_STYL_ID", StringType(), True),
        StructField("EMER_RESPNDR_FL", StringType(), True),
        StructField("OWNR_ZIP", IntegerType(), True),
        StructField("FIN_RESP_PROOF_ID", IntegerType(), True),
        StructField("FIN_RESP_TYPE_ID", StringType(), True),
        StructField("VEH_DMAG_AREA_1_ID", StringType(), True),
        StructField("VEH_DMAG_SCL_1_ID", StringType(), True),
        StructField("FORCE_DIR_1_ID", IntegerType(), True),
        StructField("VEH_DMAG_AREA_2_ID", StringType(), True),
        StructField("VEH_DMAG_SCL_2_ID", StringType(), True),
        StructField("FORCE_DIR_2_ID", IntegerType(), True),
        StructField("VEH_INVENTORIED_FL", StringType(), True),
        StructField("VEH_TRANSP_NAME", StringType(), True),
        StructField("VEH_TRANSP_DEST", StringType(), True),
        StructField("CONTRIB_FACTR_1_ID", StringType(), True),
        StructField("CONTRIB_FACTR_2_ID", StringType(), True),
        StructField("CONTRIB_FACTR_P1_ID", StringType(), True),
        StructField("VEH_TRVL_DIR_ID", StringType(), True),
        StructField("FIRST_HARM_EVT_INV_ID", StringType(), True),
        StructField("INCAP_INJRY_CNT", IntegerType(), True),
        StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
        StructField("POSS_INJRY_CNT", IntegerType(), True),
        StructField("NON_INJRY_CNT", IntegerType(), True),
        StructField("UNKN_INJRY_CNT", IntegerType(), True),
        StructField("TOT_INJRY_CNT", IntegerType(), True),
        StructField("DEATH_CNT", IntegerType(), True)
    ])
    return schema

