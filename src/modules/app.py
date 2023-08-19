from src.modules.abstract_app import AbstractApp


class app(AbstractApp):

    def __int__(self):
        super(self).__int__()

    def __enter__(self):
        """ creating Spark Session"""
        app_name = f"pyspark_demo_{env}"