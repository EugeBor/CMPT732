from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import re, string
conf = SparkConf().setAppName('nasa logs')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def main(inputs, output):
	text = sc.textFile(inputs)
	logs = text.flatMap(words_once)
	nasa_schema = types.StructType([
	    types.StructField('host', types.StringType(), False),
	    types.StructField('bytes', types.IntegerType(), False),
	])
	dfLogs = spark.createDataFrame(logs, nasa_schema)
	dfLogs.createOrReplaceTempView("dfLogs")
	numbers = dfLogs.groupby('host').agg(functions.count('host').alias('x'), functions.sum('bytes').alias('y'))
	numbers = numbers.withColumn('x2', numbers.x**2).withColumn('y2', numbers.y**2).withColumn('xy', numbers.x*numbers.y)
	numbers = numbers.agg(functions.count('host').alias('n'), functions.sum('x').alias('x'), functions.sum('y').alias('y'), functions.sum('x2').alias('x2'), functions.sum('y2').alias('y2'), functions.sum('xy').alias('xy'))
	r2 = numbers.withColumn('r2', (numbers.n*numbers.xy - numbers.x*numbers.y)**2/((numbers.n*numbers.x2-numbers.x**2)*(numbers.n*numbers.y2-numbers.y**2))).select('r2')
	r2.write.csv(output, compression='gzip', mode='overwrite')

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def words_once(line):
    w = wordsep.split(line)
    if len(w) > 4: 
        yield (w[1], int(w[4]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
