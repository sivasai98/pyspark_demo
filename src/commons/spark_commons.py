from src.constants.constants import SPARK_CONFIG


def get_spark_session(appname, sec_name):
    """
    Create a spark session variable
    :param appname:
    :param sec_name:
    :return:
    """
    print("Creating Spark Session")
    print(f"using the details from : {SPARK_CONFIG+sec_name}")
    # spark_builder = SparkSession