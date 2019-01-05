from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json


def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType(), False),
        types.StructField('date', types.StringType(), False),
        types.StructField('observation', types.StringType(), False),
        types.StructField('value', types.IntegerType(), False),
        types.StructField('mflag', types.StringType(), False),
        types.StructField('qflag', types.StringType(), False),
        types.StructField('sflag', types.StringType(), False),
        types.StructField('obstime', types.StringType(), False)
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather_filtered = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA')).filter(weather.observation == 'TMAX')
    weather_output = weather_filtered.withColumn('tmax', weather.value/10).select('station', 'date', 'tmax')
    weather_write = weather_output.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)