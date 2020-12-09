
"""### Docker container part II"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.ml.classification import RandomForestClassifier,RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
import sys

def data_load_and_transform(path):
  dataset = spark.read.option( 'delimiter',';').csv(path,inferSchema=True, header =True)
  new_columns_name_list=list(map(lambda x : x.replace('"',''),dataset.columns))
  dataset=dataset.toDF(*new_columns_name_list)
  feature_select_list=[dataset.columns[i] for i in [10,9,1,2]]##### Feature selection
  feature_list = []
  for col in dataset.columns:
    # if col == 'quality':
    if col not in feature_select_list:
      continue
    else:
      feature_list.append(col)

  assembler=VectorAssembler(inputCols=feature_list,outputCol='features')
  df=assembler.transform(dataset)
  return df
  
model_load=RandomForestClassificationModel.load('s3://dataset-cs643-proj/trainingModel')

# path='s3://dataset-cs643-proj/ValidationDataset.csv'
path=str(sys.argv[1])
test=data_load_and_transform(path)
predictions=model_load.transform(test)
evaluator= MulticlassClassificationEvaluator(labelCol='quality',predictionCol='prediction',metricName='f1')
accuracy=evaluator.evaluate(predictions)

print('F1 score: ',accuracy)
print('Test Error = %g' % (1.0-accuracy))
print('Final predictions:')

print(predictions.select('prediction').show(10000))

