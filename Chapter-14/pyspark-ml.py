import argparse
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def train_model(args):
    # Input arguments
    processed_data_path = args.processed_data_path
    model_path = args.model_path
    
    # Initialize Spark Session
    spark = SparkSession.builder.appName("TitanicSurvivalPrediction").getOrCreate()

    ### DATA PREPARATION SECTION ###
    
    # Read Parquet files into a DataFrame
    data = spark.read.parquet(processed_data_path)

    print(f"Data loaded successfully from {processed_data_path}")

    # Separate the target and input features in the dataset
    data = data.withColumnRenamed('Survived', 'label')

    # Split the data into training and testing sets
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    ### MODEL TRAINING AND EVALUATION SECTION ###

    # Define the pipeline stages
    assembler = VectorAssembler(inputCols=[col for col in data.columns if col != 'label'], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="label")

    # Construct the pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    # Fit the pipeline to training data
    model = pipeline.fit(train_data)

    # Make predictions on test data
    predictions = model.transform(test_data)

    # Evaluate the model
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
    print(f"AUC: {auc}")

    # Save the model locally
    model.write().overwrite().save(model_path)

    # Return the trained model
    return model

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train a logistic regression model for Titanic survival prediction')

    parser.add_argument('--processed_data_path', type=str, help='Path to the directory containing the preprocessed data')
    parser.add_argument('--model_path', type=str, help='Path to save the trained model')

    args = parser.parse_args()

    train_model(args)
