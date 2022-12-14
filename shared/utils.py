from pyspark.sql import SparkSession
import json


def create_spark_session():
    """Initializes the Spark session object.
    Returns:
        The Spark session object.
    """
    spark = SparkSession.builder.appName("bcg_case").getOrCreate()
    return spark


def get_configs(config):
    """Extract parameters from json config file.
    Args:
         File path of json file
    Returns:
        input/output paths
    """
    with open(f"{config}", "r") as para:
        config = json.load(para)
    return config


def read_csv(spark,path):
    """Reads data from csv file.
    Args:
         Spark Session
         File path of csv file
    Returns:
        Spark Dataframe with unique records
    """
    return spark.read.load(path, format="csv", header=True, inferSchema=True).dropDuplicates()


def write_csv(df,spark,path):
    """Writes data from spark df to csv files.
     Args:
          Spark Session
          File path to save csv file
     """
    df.coalesce(1).write.mode("overwrite").save(path, format("csv"), header=True)


