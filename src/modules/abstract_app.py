import abc

from src.commons.property_reader import get_property
from src.commons.spark_read_write_handler import ReadWiteHandler
from src.constants.property_constants import SEC_COMMON, MOVIES_DAT_FILE_PATH, RATINGS_DAT_FILE_PATH, \
    USERS_DAT_FILE_PATH


class AbstractApp(abc.ABC):

    def __init__(self, env):
        self.env = env

    @property
    def movies_dat_file_path(self):
        return get_property(SEC_COMMON, MOVIES_DAT_FILE_PATH)

    @property
    def ratings_dat_file_path(self):
        return get_property(SEC_COMMON, RATINGS_DAT_FILE_PATH)

    @property
    def users_dat_file_path(self):
        return get_property(SEC_COMMON, USERS_DAT_FILE_PATH)

    def read_data(self, read_write_handler: ReadWiteHandler):
        """
        read dat files from HDFS
        :param read_write_handler:
        :return: Tuple of DataFrame
        """
        pass

    def do(self):
        """
        Process Data and write to HDFS
        :return: None
        """
        pass
