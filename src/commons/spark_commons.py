from src.commons.property_reader import get_section_config
from src.constants.constants import SPARK_CONFIG
from pyspark.sql import SparkSession


def get_spark_session(app_name, env):
    """
    Create a spark session variable
    :param appname:
    :param sec_name:
    :return:
    """
    print("Creating Spark Session")
    sec_name = SPARK_CONFIG + env
    print(f"using the details from : {sec_name}")
    spark_builder = SparkSession.builder.appName(app_name)
    spark_confs = get_section_config(sec_name)
    for key, val in spark_confs:
        spark_builder.config(key, val)
    spark_session = spark_builder.getOrCreate()
    return spark_session
