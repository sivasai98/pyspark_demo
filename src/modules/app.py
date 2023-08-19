from src.commons.property_reader import get_property
from src.constants.property_constants import SEC_COMMON
from src.modules.abstract_app import AbstractApp


class App(AbstractApp):

    def __init__(self, env):
        super(App,self).__init__(env)

    def __enter__(self):
        """ creating Spark Session"""
        app_name = f"pyspark_demo_{self.env}"

        print(app_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ closing the spark context!"""
        print("spark session closed!!!")

    def do(self):
        print("working")
        get_property(SEC_COMMON,"a")
