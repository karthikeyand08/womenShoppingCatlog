# encoding: utf-8

import configargparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # split, explode, collect_list
from pyspark.sql.window import Window


class WSCsvStream:

    def __init__(self, input_file, redis_host, redis_port, jars):

        if not input_file:
            raise ValueError("'input_file' parameter cannot be None")

        self.input_file = input_file

        self.sc = SparkSession.builder \
            .config('spark.jars', jars) \
            .config("spark.redis.host", redis_host) \
            .config("spark.redis.port", redis_port) \
            .appName('MAD') \
            .getOrCreate()
        self.__base_df = None

    @staticmethod
    def write_to_redis(_df, t_val, c_val):
        _df.write.format("org.apache.spark.sql.redis") \
            .mode('overwrite') \
            .option("table", t_val) \
            .option("key.column", c_val).save()
        _df.show(20, False)

    def get_base_df(self):
        if self.__base_df:
            return self.__base_df

        df = self.sc.read.csv(
            self.input_file, header=True, inferSchema=True
        ).select('id', 'brand', 'colors', 'dateAdded').na.drop()
        df = df.dropDuplicates(['id'])

        self.__base_df = df.withColumn('date', df['dateAdded'].cast('date'))
        self.write_to_redis(self.__base_df, "base_df", "dateAdded")
        return self.__base_df

    def stream_recent_items_by_date(self):
        base_df = self.get_base_df()
        latest_date_df = base_df.groupBy('date').agg({'dateAdded': 'max'})
        latest_date_df = latest_date_df.orderBy('date')
        self.write_to_redis(latest_date_df, "recent_df", "date")

    def stream_brand_count_by_date(self):
        base_df = self.get_base_df()
        brand_count_df = base_df.groupBy('date', 'brand').agg(
            {'brand': 'count'}
        )

        mergeCols = F.udf(lambda date, brand: '{}_{}'.format(date, brand))
        brand_count_df = brand_count_df.withColumn(
            'nick_name',
            mergeCols(F.col('date'), F.col('brand'))

        )

        brand_count_df = brand_count_df.orderBy('date')
        self.write_to_redis(brand_count_df, "brand_count", "nick_name")

    def stream_top_10_by_color(self):
        base_df = self.get_base_df()
        base_df = base_df.withColumn(
            'colors',
            F.split(base_df['colors'], ',|/').cast('array<string>')
        )
        color_df = base_df.withColumn('colors', F.explode('colors'))
        color_df = color_df.dropDuplicates()

        window = Window.partitionBy(color_df['colors']).orderBy(
            color_df['dateAdded'].desc())

        color_df = color_df.select('*',
                                   F.row_number().over(window).alias('rank'))\
            .filter(F.col('rank') <= 10)

        tune_data = F.udf(
            lambda colors: colors.lower().strip().replace(' ', '_')
        )

        color_df = color_df.withColumn(
            'colors',
            tune_data(F.col('colors'))
        )

        mergeCols = F.udf(
                lambda _id, colors: '{}___{}_colors'.format(_id, colors)
        )
        color_df = color_df.withColumn(
            'nick_name',
            mergeCols(F.col('id'), F.col('colors')))

        self.write_to_redis(color_df, "color_df", "nick_name")


if __name__ == '__main__':
    c = configargparse.ArgumentParser()
    c.add_argument('-if', '--input-file', env_var='INPUT_FILE', required=True)
    c.add_argument('-jrs', '--jars', env_var='JARS', required=True)
    c.add_argument('-rh', '--redis-host', env_var='REDIS_HOST',
                   default='localhost')
    c.add_argument('-rp', '--redis-port', env_var='REDIS_PORT', default='6379')

    options = c.parse_args()

    ws_inst = WSCsvStream(
        input_file=options.input_file,
        redis_host=options.redis_host,
        redis_port=options.redis_port,
        jars=options.jars
    )

    ws_inst.stream_recent_items_by_date()
    ws_inst.stream_brand_count_by_date()
    ws_inst.stream_top_10_by_color()
    print('EOE')

