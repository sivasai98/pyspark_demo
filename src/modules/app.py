from src.commons.property_reader import get_property
from src.commons.spark_commons import get_spark_session
from src.constants.property_constants import SEC_COMMON
from src.modules.abstract_app import AbstractApp


class App(AbstractApp):

    def __init__(self, env):
        super(App, self).__init__(env)

    def __enter__(self):
        """ creating Spark Session"""
        app_name = f"pyspark_demo_{self.env}"
        if 'spark' not in globals():
            print('No Spark Session exists in prior')
            spark = None
        self.spark = get_spark_session(app_name, self.env)

        print(app_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ closing the spark context!"""
        self.spark.stop()
        print("spark session closed!!!")

    def do(self):
        pass
