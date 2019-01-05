from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('weather').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

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

    weather = spark.read.csv(inputs, schema=observation_schema).cache()
    weather.createOrReplaceTempView("weather")

    wmax = spark.sql("SELECT * FROM weather WHERE qflag IS NULL AND (observation = 'TMAX')")
    wmax.createOrReplaceTempView("wmax")

    wmin = spark.sql("SELECT * FROM weather WHERE qflag IS NULL AND (observation = 'TMIN')")
    wmin.createOrReplaceTempView("wmin")
    
    range = spark.sql("SELECT wmin.station, wmin.date, (wmax.value - wmin.value)/10 AS range FROM wmin, wmax WHERE wmin.station = wmax.station AND wmin.date = wmax.date")
    range.createOrReplaceTempView("range")

    max = spark.sql("SELECT date, MAX(range) AS range FROM range GROUP BY date")
    max.createOrReplaceTempView("max")

    stations = spark.sql("SELECT range.station, range.date, range.range FROM range, max WHERE range.date = max.date AND range.range = max.range")
    stations.createOrReplaceTempView("stations")

    stations.write.csv(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)