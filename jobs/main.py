from shared.utils import create_spark_session, get_configs, read_csv, write_csv
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window


def analysis_1(primary_person, output_path):
    """Find the number of crashes (accidents) in which number of persons killed are male?
    """
    out_df = primary_person.filter((col("PRSN_GNDR_ID") == "MALE") & (col("PRSN_INJRY_SEV_ID") == "KILLED")) \
        .select("CRASH_ID").distinct()

    print("Number of crashes in which persons killed are male:", out_df.count())


def analysis_2(units, output_path):
    """How many two wheelers are booked for crashes?
    """
    out_df = units.filter((col("VEH_BODY_STYL_ID").isin("MOTORCYCLE", "POLICE MOTORCYCLE")) \
                          & col("vin").isNotNull()).select("vin").distinct()

    print("Number of two wheelers booked for crashes:", out_df.count())


def analysis_3(primary_person, output_path):
    """Which state has the highest number of accidents in which females are involved?
    """
    out_df = primary_person.filter(col("PRSN_GNDR_ID") == "FEMALE") \
        .groupBy("DRVR_LIC_STATE_ID").count().orderBy(col("COUNT").desc()) \
        .select("DRVR_LIC_STATE_ID").limit(1)

    print("State with highest number of accidents in which females are involved:")
    out_df.show()


def analysis_4(primary_person, units, output_path):
    """Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number
       of injuries including death?
    """
    windowSpec = Window.orderBy(col("count").desc())

    out_df = primary_person.join(units, primary_person.CRASH_ID == units.CRASH_ID, "inner") \
        .filter((col("PRSN_INJRY_SEV_ID").isin("KILLED", "NON-INCAPACITATING INJURY", "POSSIBLE INJURY",
                                               "INCAPACITATING INJURY")) \
                & (col("VEH_MAKE_ID") != "NA")) \
        .groupBy("VEH_MAKE_ID").count()

    out_df = out_df.withColumn("rank", rank().over(windowSpec))
    out_df = out_df.filter(col("RANK").between(5, 15)).select("VEH_MAKE_ID")

    print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number  \
       of injuries including death:")

    out_df.show()


def analysis_5(primary_person, units, output_path):
    """For all the body styles involved in crashes, mention the top ethnic
    user group of each unique body style
    """

    out_df = primary_person.join(units, primary_person.CRASH_ID == units.CRASH_ID, "inner") \
        .groupBy(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")).count()

    windowSpec = Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("count").desc())
    out_df = out_df.withColumn("rank", rank().over(windowSpec))
    out_df = out_df.filter(col("RANK") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

    print("The top ethnic user group of each unique body style involved in crash:")

    out_df.show()


def analysis_6(primary_person, units, output_path):
    """Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes
    with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    """

    out_df = primary_person.join(units, primary_person.CRASH_ID == units.CRASH_ID, "inner") \
        .filter((col("VEH_BODY_STYL_ID").isin("VAN", "SPORT UTILITY VEHICLE", "PASSENGER CAR, 4-DOOR",
                                              "POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR",
                                              "NEV-NEIGHBORHOOD ELECTRIC VEHICLE"))
                & (col("PRSN_ALC_RSLT_ID") == "Positive") & (col("DRVR_ZIP").isNotNull())) \
        .groupBy("DRVR_ZIP").count().orderBy(col("count").desc()).select("DRVR_ZIP").limit(5)

    print(" The Top 5 Zip Codes with highest number crashes  \
    with alcohols as the contributing factor to a crash:")

    out_df.show()


def analysis_7(units, damages, output_path):
    """Count of Distinct Crash IDs where No Damaged Property was observed
    and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    """
    out_df = units.alias("units").join(damages.alias("damages"), units.CRASH_ID == damages.CRASH_ID, "left") \
        .filter( (col("damages.CRASH_ID").isNull()) & (col("VEH_DMAG_SCL_1_ID").isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")) \
                 & (col("VEH_DMAG_SCL_2_ID").isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")) \
                 & (col("FIN_RESP_TYPE_ID").isin("INSURANCE BINDER","LIABILITY INSURANCE POLICY","CERTIFICATE OF SELF-INSURANCE","PROOF OF LIABILITY INSURANCE")) ) \
        .select("units.CRASH_ID").distinct()

    print("Count of Distinct Crash IDs where No Damaged Property was observed \
    and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance:",out_df.count())


def analysis_8(primary_person,units,charges,output_path):
    """Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the
     Top 25 states with highest number of offences
     """

    top_10_vehicle_colours = [row.VEH_COLOR_ID for row in
                              units.filter(col("VEH_COLOR_ID") != "NA").groupBy("VEH_COLOR_ID").count() \
                                  .orderBy(col("COUNT").desc()).select("VEH_COLOR_ID").limit(10).collect()]

    top_25_offences_states = [row.VEH_LIC_STATE_ID for row in units.filter((col("VEH_BODY_STYL_ID")\
                            .isin("VAN", "SPORT UTILITY VEHICLE", "PASSENGER CAR, 4-DOOR", "POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR", "NEV-NEIGHBORHOOD ELECTRIC VEHICLE"))) \
                            .groupBy("VEH_LIC_STATE_ID").count().orderBy(col("COUNT").desc()).limit(25).select("VEH_LIC_STATE_ID").collect()]

    out_df = primary_person.join(units, primary_person.CRASH_ID == units.CRASH_ID, "inner") \
        .filter((col("DRVR_LIC_TYPE_ID").isin("COMMERCIAL DRIVER LIC.", "DRIVER LICENSE")) \
                & col("VEH_COLOR_ID").isin(top_10_vehicle_colours) \
                & col("VEH_LIC_STATE_ID").isin(top_25_offences_states)) \
        .join(charges, primary_person.CRASH_ID == charges.CRASH_ID, "inner") \
        .filter(col("CHARGE").contains("SPEED")) \
        .groupBy("VEH_MAKE_ID").count().orderBy(col("COUNT").desc()).select("VEH_MAKE_ID").limit(5) \

    print("Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences:")
    out_df.show()



def create_dfs(spark, input_configs):
    """Creates all the necessary dataframes from csv files.
    Args:
         Spark Session
         Input configs with file path
    Returns:
         Spark Dataframes
    """
    primary_person_df = read_csv(spark, input_configs["PRIMARY_PERSON_FILE_PATH"])
    units_df = read_csv(spark, input_configs["UNITS_DATA_PATH"])
    charges_df = read_csv(spark, input_configs["CHARGES_FILE_PATH"])
    damages_df = read_csv(spark, input_configs["DAMAGES_FILE_PATH"])

    return primary_person_df, units_df, charges_df, damages_df


def main():
    """Reads data from csv files transforms it as per problem and save results as csv.
    """
    spark = create_spark_session()
    config = "../configs/config.json"
    input_configs = get_configs(config).get("INPUT")
    primary_person, units, charges, damages = create_dfs(spark, input_configs)
    output_configs = get_configs(config).get("OUTPUT")

    # Solving analytics problems
    analysis_1(primary_person, output_configs["ANALYSIS_1"])
    analysis_2(units, output_configs["ANALYSIS_2"])
    analysis_3(primary_person, output_configs["ANALYSIS_3"])
    analysis_4(primary_person, units, output_configs["ANALYSIS_4"])
    analysis_5(primary_person, units, output_configs["ANALYSIS_5"])
    analysis_6(primary_person, units, output_configs["ANALYSIS_6"])
    analysis_7(units, damages, output_configs["ANALYSIS_7"])
    analysis_8(primary_person,units,charges,output_configs["ANALYSIS_8"])



if __name__ == '__main__':
    main()
