from pyspark.sql import SparkSession, functions, types
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('weather').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station_ID', types.StringType(), False),
        types.StructField('date', types.StringType(), False),
        types.StructField('observation', types.StringType(), False),
        types.StructField('value', types.IntegerType(), False),
        types.StructField('mflag', types.StringType(), False),
        types.StructField('qflag', types.StringType(), False),
        types.StructField('sflag', types.StringType(), False),
        types.StructField('obstime', types.StringType(), False)
    ])

    weather = spark.read.csv(inputs, schema=observation_schema).cache()
    wmax = weather.filter(weather.observation == 'TMAX').withColumn('tmax', weather.value/10).select('station_ID', 'date', 'tmax')
    wmin = weather.filter(weather.observation == 'TMIN').withColumn('tmin', weather.value/10).select('station_ID', 'date', 'tmin')
    range = wmax.join(wmin, ['station_ID', 'date']).withColumn('range', wmax.tmax - wmin.tmin).select('station_ID', 'date', 'range').cache()
    max_range = range.groupBy('date').max('range')
    max = max_range.withColumn('range', max_range['max(range)']).select('date', 'range')
    stations = range.join(max, ['date', 'range']).select('date', 'station_ID', 'range')
    stations.write.csv(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)