import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor


def main(inputs, output):
	tmax_schema = types.StructType([
		types.StructField('station', types.StringType()),
		types.StructField('date', types.DateType()),
		types.StructField('latitude', types.FloatType()),
		types.StructField('longitude', types.FloatType()),
		types.StructField('elevation', types.FloatType()),
		types.StructField('tmax', types.FloatType()),
	])
	data = spark.read.csv(inputs, schema=tmax_schema)


	query = "SELECT t.station AS station, t.date AS date, t.day AS day, t.latitude AS latitude, t.longitude AS longitude, t.elevation AS elevation, t.tmax AS tmax, y.tmax AS tmax_yesterday FROM (SELECT station, date, latitude, longitude, elevation, tmax, DAYOFYEAR(date) AS day, date_sub(date,1) AS date_yesterday FROM __THIS__) t, (SELECT station, date, latitude, longitude, elevation, tmax, DAYOFYEAR(date) AS day, date_sub(date,1) AS date_yesterday FROM __THIS__) y WHERE t.date = y.date_yesterday AND t.station = y.station"
	sqlTrans = SQLTransformer(statement=query)
	train, validation = data.randomSplit([0.75, 0.25])
	train = train.cache()
	# train.show()
	validation = validation.cache()
	assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day", "tmax_yesterday"], outputCol="features")
	classifier = GBTRegressor(featuresCol='features', labelCol='tmax')
	pipeline = Pipeline(stages=[sqlTrans, assembler, classifier])
	model = pipeline.fit(train)
	predictions = model.transform(validation)
	predictions.show()

	r2_evaluator = RegressionEvaluator(
		predictionCol='prediction', labelCol='tmax',
		metricName='r2')
	r2 = r2_evaluator.evaluate(predictions)

	print("R-square for the validation data is: " + str(r2))
	model.write().overwrite().save(output)

	r2 = r2_evaluator.evaluate(model.transform(train))
	print("R-square for the training data is: " + str(r2))

	print(model.stages[-1].featureImportances)

	sfu_predict = [("sfu", datetime.date(2018, 11, 12), 49.2771, -122.9146, 330.0, 12.0), ("sfu", datetime.date(2018, 11, 13), 49.2771, -122.9146, 330.0, 12.0)]
	sfu_predict_df = spark.createDataFrame(sfu_predict, schema=tmax_schema)
	sfu_predict_df.show()
	sfu_predictions = model.transform(sfu_predict_df).select('station', 'date', 'prediction')
	sfu_predictions.show()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)