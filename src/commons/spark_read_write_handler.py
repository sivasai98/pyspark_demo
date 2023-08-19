from src.constants.property_constants import CSV, HEADER, IGNORETRAILINGWHITESPACE, \
    DELIMITER, DAT_FILE_DELIMITER, INFERSCHEMA


class ReadWiteHandler:
    def __init__(self, spark):
        self.spark = spark

    def read_dat_files(self, path):
        """
        read the dat file from the given path
        :param path:
        :return: dataframe
        """
        path = "hdfs://localhost:9000"+path
        dat_df = self.spark.read.format(CSV)\
            .option(HEADER, True)\
            .option(DELIMITER, DAT_FILE_DELIMITER)\
            .option(IGNORETRAILINGWHITESPACE, True)\
            .option(INFERSCHEMA,True)\
            .load(path)
        return dat_df
