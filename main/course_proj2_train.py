
from pyspark.ml.feature import VectorAssembler,MinMaxScaler,StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier,RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

### Read the data && data preprocessing
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
  
dataset='s3://dataset-cs643-proj/TrainingDataset.csv'
dataset=data_load_and_transform(dataset)
test='s3://dataset-cs643-proj/ValidationDataset.csv'
test=data_load_and_transform(test)


### Setup multiple instances for later training
from multiprocessing.pool import ThreadPool
pool=ThreadPool(5)


### Model train && predict
rf = RandomForestClassifier(labelCol='quality', featuresCol='features',\
                            numTrees=20,maxDepth=6,seed=10,impurity='entropy')

model=rf.fit(dataset)
predictions=model.transform(test)

evaluator= MulticlassClassificationEvaluator(labelCol='quality',predictionCol='prediction',\
                                             metricName='f1')
accuracy=evaluator.evaluate(predictions)
print('F1 score: ',accuracy)
print('Test Error = %g' % (1.0-accuracy))

### Output the trained model for future testing
model.save('s3://dataset-cs643-proj/trainingModel')



