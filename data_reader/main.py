from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import countDistinct
from pyspark.sql.window import Window
from model import Charges, PrimaryPerson, Units, Damages

import json


def save_data(df, filename, config):
    df.coalesce(1).write.mode("overwrite").csv(
        f"{config.get('output_data_path')}/{filename}"
    )


def analytics_1(spark, config):
    person = PrimaryPerson.extract_data(spark, config)
    person = person.na.drop(subset=["CRASH_ID"])
    male_killed = person \
        .filter((person.PRSN_INJRY_SEV_ID == 'KILLED') & (person.PRSN_GNDR_ID == 'MALE'))

    print("Output for query 1(number of accidents where males killed)")
    male_killed \
        .select(countDistinct("CRASH_ID").alias("no of accidents where males killed ")) \
        .show()
    save_data(df=male_killed, filename="query1", config=config)
    return male_killed


def analytics_2(spark, config):
    units = Units.extract_data(spark, config)
    units = units.na.drop(subset=["CRASH_ID"])
    two_wheelers = units.filter(
        (units.VEH_BODY_STYL_ID == "MOTORCYCLE") | (units.VEH_BODY_STYL_ID == "POLICE MOTORCYCLE"))
    two_wheelers_booked_for_crash = two_wheelers \
        .select(countDistinct("CRASH_ID").alias("2WheelersBookedForCrash"))

    print("Output for query 2(number of accidents where 2 wheelers booked for crash)")
    two_wheelers_booked_for_crash.show()

    save_data(df=two_wheelers_booked_for_crash, filename="query2", config=config)
    return two_wheelers_booked_for_crash


def analytics_3(spark, config):
    primary_person = PrimaryPerson.extract_data(spark, config)
    primary_person = primary_person.na.drop(subset=["DRVR_LIC_STATE_ID"]).drop(primary_person.DRVR_LIC_STATE_ID == 'NA')
    state_with_highest_women_involved = primary_person \
        .filter(primary_person.PRSN_GNDR_ID == "FEMALE") \
        .groupBy(primary_person.DRVR_LIC_STATE_ID) \
        .count() \
        .withColumnRenamed("count", "total_cases") \
        .orderBy(F.col("total_cases").desc()) \
        .limit(1)

    print("Output for query 3(state with highest number of females involved in crash )")
    state_with_highest_women_involved.show()
    save_data(df=state_with_highest_women_involved, filename="query3", config=config)
    return state_with_highest_women_involved


def analytics_4(spark, config):
    units = Units.extract_data(spark, config)
    units = units.na.drop(subset=["CRASH_ID"])
    vehicles = units \
        .groupBy(units.VEH_MAKE_ID) \
        .agg(F.sum(units.TOT_INJRY_CNT + units.DEATH_CNT).alias("vehicle_crash_count")) \
        .orderBy(F.col("vehicle_crash_count").desc())

    window_spec = Window.orderBy(F.col("vehicle_crash_count").desc())
    vehicles_with_highest_crash = vehicles \
        .withColumn("dense_rank", F.dense_rank().over(window_spec).alias("rank")) \
        .filter((F.col("rank") >= 5) & (F.col("rank") <= 15))

    print("Output for query 4(the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries )")
    vehicles_with_highest_crash.show()
    save_data(df=vehicles_with_highest_crash, filename="query4", config=config)
    return vehicles_with_highest_crash


def analytics_5(spark, config):
    units = Units.extract_data(spark, config)
    person = PrimaryPerson.extract_data(spark, config)
    units.na.drop(subset=["CRASH_ID", "UNIT_NBR", "VEH_BODY_STYL_ID"]).drop(units.VEH_BODY_STYL_ID == "NA")
    person.na.drop(subset=["CRASH_ID", "UNIT_NBR", "PRSN_ETHNICITY_ID"])

    cond = [units.CRASH_ID == person.CRASH_ID, units.UNIT_NBR == person.UNIT_NBR]
    joined_df = units \
        .join(person, cond, 'inner') \
        .select(units.VEH_BODY_STYL_ID, person.PRSN_ETHNICITY_ID) \
        .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
        .count() \
        .withColumnRenamed("count", "total_count")

    window = Window.partitionBy(joined_df.VEH_BODY_STYL_ID).orderBy(F.col("total_count").desc())
    ethnic_group_body_type = joined_df \
        .withColumn("dense_rank", F.dense_rank().over(window).alias("rank")) \
        .filter((F.col("rank") == 1)) \
        .drop("dense_rank")

    print("Output for query 5(: For all the body styles involved in crashes, "
          "mention the top ethnic user group of each unique body style)")
    ethnic_group_body_type.show()
    save_data(df=ethnic_group_body_type, filename="query5", config=config)
    return ethnic_group_body_type


def analytics_6(spark, config):
    person = PrimaryPerson.extract_data(spark, config)
    person = person.na.drop(subset=["CRASH_ID", "UNIT_NBR", "DRVR_ZIP"])
    top5_driver_zip_codes_with_crash = person \
        .filter(person.PRSN_ALC_RSLT_ID == 'Positive') \
        .groupBy("DRVR_ZIP").count() \
        .withColumnRenamed("count", "total_count") \
        .orderBy(F.col("total_count").desc()) \
        .limit(5)

    print("Output for query 6(Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes "
          "with alcohols as the contributing factor to a crash (Use Driver Zip Code))")
    top5_driver_zip_codes_with_crash.show()
    save_data(df=top5_driver_zip_codes_with_crash, filename="query6", config=config)
    return top5_driver_zip_codes_with_crash


def analytics_7(spark, config):
    units = Units.extract_data(spark, config)
    units = units.na.drop(subset=["CRASH_ID", "UNIT_NBR"])

    vehicles_with_dmg_4_insurance = units \
        .filter((F.col("FIN_RESP_PROOF_ID") == 1) & (F.col("VEH_DMAG_SCL_1_ID").rlike("(4|5|6|7)"))) \
        .select(F.col("CRASH_ID")) \
        .distinct()

    damages = Damages.extract_data(spark, config)

    damages = damages.na.drop(subset=["CRASH_ID"])
    damages = damages \
        .select(F.col("CRASH_ID")) \
        .distinct()

    no_property_damaged = vehicles_with_dmg_4_insurance \
        .subtract(damages) \
        .count()

    print("Output for query 7(Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level"
          " (VEH_DMAG_SCL~) is above 4 and car avails Insurance)")

    print(no_property_damaged)
    return no_property_damaged


def analytics_8(spark, config):
    charges = Charges.extract_data(spark, config)
    charges = charges.na.drop(subset=["CRASH_ID", "UNIT_NBR", "PRSN_NBR"])

    units = Units.extract_data(spark, config)
    units = units.na.drop(subset=["CRASH_ID", "UNIT_NBR", "VEH_LIC_STATE_ID", "VEH_COLOR_ID", "VEH_MAKE_ID"])

    top_25_states = units. \
        filter(F.col("VEH_LIC_STATE_ID") != "NA") \
        .groupBy("VEH_LIC_STATE_ID") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(25)

    top_10_used_colors = units \
        .filter(F.col("VEH_COLOR_ID") != "NA") \
        .groupBy("VEH_COLOR_ID") \
        .count().orderBy(F.col("count").desc()) \
        .limit(10)

    speeding_related_charges = charges.filter(F.col("CHARGE").rlike("speed|SPEED"))

    person = PrimaryPerson.extract_data(spark, config)
    person = person.na.drop(subset=["CRASH_ID", "UNIT_NBR", "PRSN_NBR", "DRVR_LIC_TYPE_ID"])
    person_with_driver_license = person \
        .filter((F.col("DRVR_LIC_TYPE_ID").like("%DRIVER LICENSE%")) | (
        F.col("DRVR_LIC_TYPE_ID").like("%COMMERCIAL DRIVER LIC.%")))

    cond1 = [person_with_driver_license.CRASH_ID == speeding_related_charges.CRASH_ID,
             person_with_driver_license.UNIT_NBR == speeding_related_charges.UNIT_NBR,
             person_with_driver_license.PRSN_NBR == speeding_related_charges.PRSN_NBR]

    cond2 = [person_with_driver_license.CRASH_ID == units.CRASH_ID,
             person_with_driver_license.UNIT_NBR == units.UNIT_NBR]

    final_df = person_with_driver_license \
        .join(speeding_related_charges, cond1, "inner") \
        .join(units, cond2, 'inner') \
        .filter((F.col("VEH_COLOR_ID").isin(top_10_used_colors.VEH_COLOR_ID)) & (
        F.col("VEH_LIC_STATE_ID").isin(top_25_states.VEH_LIC_STATE_ID))) \
        .groupBy(units.VEH_MAKE_ID) \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(5)

    print(
        "Output for query 8(Determine the Top 5 Vehicle Makes where drivers are charged with "
        "speeding related offences, "
        "has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with "
        "highest number of offences (to be deduced from the data))")
    final_df.show()
    save_data(df=final_df, filename="query8", config=config)
    return final_df


def main():
    """ Main function excecuted by spark-submit command"""

    with open("./data_reader/config.json", "r") as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()

    analytics_1(spark, config)
    analytics_2(spark, config)
    analytics_3(spark, config)
    analytics_4(spark, config)
    analytics_5(spark, config)
    analytics_6(spark, config)
    analytics_7(spark, config)
    analytics_8(spark, config)


if __name__ == '__main__':
    main()
