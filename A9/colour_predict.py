import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    train.show()
    validation = validation.cache()

    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    word_indexer = StringIndexer(inputCol="word", outputCol="label")
    classifier = MultilayerPerceptronClassifier(layers=[3, 30, 11]) # was [3, 25, 25], but updated -GB


    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    plot_predictions(rgb_model, 'RGB', labelCol='word')

    rgb_predictions = rgb_model.transform(validation)
    rgb_predictions.show()
    rgb_score_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    rgb_score = rgb_score_evaluator.evaluate(rgb_predictions)

    print('Validation score for RGB model:', rgb_score)

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement = rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='features')
    lab_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)
    plot_predictions(lab_model, 'LAB', labelCol='word')

    lab_predictions = lab_model.transform(validation)
    lab_predictions.show()
    lab_score_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    lab_score = lab_score_evaluator.evaluate(lab_predictions)

    print('Validation score for LAB model:', lab_score)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)