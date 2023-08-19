from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from src.commons.property_reader import get_property
from src.commons.spark_commons import get_spark_session
from src.commons.spark_read_write_handler import ReadWiteHandler
from src.constants.constants import MOVIEID, TITLE, YEAR, RATING, PERCENTAGE, ONE, TWO, THREE, FOUR, FIVE, ONE_PERCENT, \
    TWO_PERCENT, THREE_PERCENT, FOUR_PERCENT, FIVE_PERCENT, COUNT, M, F, MALE, FEMALE, GENDER, TWO_FIFTY, USERID, CNT
from src.constants.property_constants import SEC_COMMON, MOVIES_DAT_FILE_PATH, RATINGS_DAT_FILE_PATH, \
    USERS_DAT_FILE_PATH
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

    @property
    def movies_dat_file_path(self):
        return get_property(SEC_COMMON, MOVIES_DAT_FILE_PATH)

    @property
    def ratings_dat_file_path(self):
        return get_property(SEC_COMMON, RATINGS_DAT_FILE_PATH)

    @property
    def users_dat_file_path(self):
        return get_property(SEC_COMMON, USERS_DAT_FILE_PATH)

    def read_data(self):
        read_write_handler = ReadWiteHandler(self.spark)
        movies_df = read_write_handler.read_dat_files(self.movies_dat_file_path) \
            .withColumn(YEAR, f.regexp_extract(f.col(TITLE), "\\((\\d+)\\)", 1).cast(IntegerType()))
        rating_df = read_write_handler.read_dat_files(self.ratings_dat_file_path)
        users_df = read_write_handler.read_dat_files(self.users_dat_file_path)
        return movies_df, rating_df, users_df

    def do(self):
        read_write_handler = ReadWiteHandler(self.spark)
        movies_df, ratings_df, users_df = self.read_data(read_write_handler)

        data_df = ratings_df.join(users_df, USERID).join(movies_df, MOVIEID)

        data_df.persist()

        movies_rating_df = data_df.repartition(f.col(MOVIEID)).groupBy(TITLE, RATING).agg(
            f.count(RATING).alias(CNT)) \
            .selectExpr(TITLE, RATING, "round((cnt / (sum(cnt) over(partition by title)))*100,3) as percentage") \
            .groupBy(TITLE).pivot(RATING).agg(f.max(PERCENTAGE)) \
            .na.fill(0) \
            .withColumnRenamed(ONE, ONE_PERCENT) \
            .withColumnRenamed(TWO, TWO_PERCENT) \
            .withColumnRenamed(THREE, THREE_PERCENT) \
            .withColumnRenamed(FOUR, FOUR_PERCENT) \
            .withColumnRenamed(FIVE, FIVE_PERCENT)
        movies_rating_df.show(5,False)

        mean_ratings_pivot = data_df.repartition(f.col(MOVIEID)).groupBy(TITLE) \
            .pivot(GENDER).agg(f.round(f.mean(RATING), int(THREE)))

        mean_ratings_pivot.persist()

        ratings_by_title_df = data_df.groupBy(TITLE).count()

        active_titles_df = ratings_by_title_df.where(f.col(COUNT) >= TWO_FIFTY)

        active_titles_list = [row.title for row in active_titles_df.select(TITLE).collect()]

        mean_ratings_df = mean_ratings_pivot.where(f.col(TITLE).isin(active_titles_list)) \
            .withColumnRenamed(F, FEMALE).withColumnRenamed(M, MALE)

        mean_ratings_df.show(5,False)
        mean_ratings_pivot.unpersist()
        data_df.unpersist()
