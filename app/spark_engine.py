from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas as pd

class SparkAnalyticsEngine:
    def __init__(self):
        # Initializes a local standalone Spark Session instance context
        self.spark = SparkSession.builder \
            .appName("AcmeDWHSparkEngine") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

    def calculate_batch_aggregations(self, data_records: list) -> dict:
        """[Requirement M6] Executes Spark data calculations over historical indicators."""
        if not data_records:
            return {"total_records": 0, "minimum_value": 0.0, "maximum_value": 0.0, "average_value": 0.0}
            
        # Normalize and transform dynamic mongo sub-documents into tabular structures
        flat_records = []
        for record in data_records:
            flat_records.append({
                "assetId": record["assetId"],
                "timestamp": str(record["timestamp"]),
                "close": float(record["indicators"].get("close", 0.0))
            })
            
        df = self.spark.createDataFrame(flat_records)
        metrics = df.groupBy("assetId").agg(
            count("close").alias("total"),
            min("close").alias("min_val"),
            max("close").alias("max_val"),
            avg("close").alias("avg_val")
        ).collect()
        
        if not metrics:
            return {"total_records": 0, "minimum_value": 0.0, "maximum_value": 0.0, "average_value": 0.0}
            
        return {
            "total_records": metrics[0]["total"],
            "minimum_value": metrics[0]["min_val"],
            "maximum_value": metrics[0]["max_val"],
            "average_value": round(metrics[0]["avg_val"], 2)
        }

    def execute_market_predictive_workflow(self, data_records: list) -> dict:
        """[Requirement M7] Train a Spark ML Linear Regression model to forecast the next day's price."""
        if len(data_records) < 2:
            return {"status": "Aborted", "message": "Insufficient data available to execute ML model training."}
            
        # Parse records into sequential daily tracking arrays
        sorted_records = sorted(data_records, key=lambda x: x["timestamp"])
        flat_data = []
        for index, record in enumerate(sorted_records):
            flat_data.append({
                "day_index": float(index),
                "price": float(record["indicators"].get("close", 0.0))
            })
            
        df = self.spark.createDataFrame(flat_data)
        
        # Vectorize input features for Spark ML consumption
        assembler = VectorAssembler(inputCols=["day_index"], outputCol="features")
        ml_dataset = assembler.transform(df)
        
        # Train the model
        lr = LinearRegression(featuresCol="features", labelCol="price")
        model = lr.fit(ml_dataset)
        
        # Predict the next day's index position
        next_day_idx = float(len(sorted_records))
        prediction_df = self.spark.createDataFrame([{"day_index": next_day_idx}])
        prediction_dataset = assembler.transform(prediction_df)
        
        forecast_results = model.transform(prediction_dataset).collect()
        predicted_value = forecast_results[0]["prediction"]
        
        return {
            "status": "Success",
            "model_type": "Spark ML Linear Regression",
            "trained_records_count": len(sorted_records),
            "next_day_index": next_day_idx,
            "predicted_next_price": round(predicted_value, 2)
        }