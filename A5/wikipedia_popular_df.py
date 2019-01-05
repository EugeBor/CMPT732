from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('wiki').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
	name = path.split('/')[-1]
	date = name.split('-')[1] + "-" + name.split('-')[2][0:2]
	return date

def main(inputs, output):
    wiki_schema = types.StructType([
	    types.StructField('language', types.StringType(), True),
	    types.StructField('page', types.StringType(), True),
	    types.StructField('views', types.LongType(), True),
	    types.StructField('bytes', types.LongType(), True),
    ])

    wiki = spark.read.csv(inputs, sep=' ', schema=wiki_schema).withColumn('date_hour', path_to_hour(functions.input_file_name()))
    wiki_filtered = wiki.filter(wiki.language == 'en').filter(wiki.page != 'Main Page').filter(~wiki.page.startswith("Special:")).cache()
    mv = wiki_filtered.groupBy('date_hour').max('views')
    max = mv.withColumn('views', mv['max(views)']).select('date_hour', 'views')    
    views = wiki_filtered.join(max, ['date_hour', 'views'])
    views.explain()
    views.select('date_hour', 'page', 'views').write.csv(output, compression='gzip', mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)