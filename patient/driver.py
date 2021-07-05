import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize PySpark
spark = SparkSession.builder.getOrCreate()

# Define custom schema
schema = StructType([StructField("patient_id", IntegerType(), True),
                     StructField("first_name", StringType(), True),
                     StructField("last_name", StringType(), True),
                     StructField("email", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("glucose_mg/dl_t1", FloatType(), True),
                     StructField("glucose_mg/dl_t2", FloatType(), True),
                     StructField("glucose_mg/dl_t3", FloatType(), True),
                     StructField("cancerPresent", BooleanType(), True),
                     StructField("atrophy_present", IntegerType(), True)
                    ])

# current_date would be replaced by datetime.now().strftime("%Y-%m-%d") in order to handle changing dates
current_date = datetime(2020, 10, 28).strftime("%Y-%m-%d")
patient_df = spark.read.format("csv").option("header", "true").schema(schema).load(f"data/{current_date}_patient_data.csv")

# Remove Protected Health Information
patient_df = patient_df.select("patient_id", 
                                col("glucose_mg/dl_t1").alias("mg_dl_t1"), 
                                col("glucose_mg/dl_t2").alias("mg_dl_t2"), 
                                col("glucose_mg/dl_t3").alias("mg_dl_t3"), 
                                col("cancerPresent").alias("cancer_present"), 
                                "atrophy_present")

# Convert invalid data to -2.0 and filter out records accordingly
patient_df = patient_df.select("patient_id",
                               when((patient_df.mg_dl_t1 <= 0) | (patient_df.mg_dl_t1 > 999), -2).otherwise(patient_df.mg_dl_t1).alias("mg_dl_t1"),
                               when((patient_df.mg_dl_t2 <= 0) | (patient_df.mg_dl_t2 > 999), -2).otherwise(patient_df.mg_dl_t2).alias("mg_dl_t2"),
                               when((patient_df.mg_dl_t3 <= 0) | (patient_df.mg_dl_t3 > 999), -2).otherwise(patient_df.mg_dl_t3).alias("mg_dl_t3"),
                               when(patient_df.cancer_present == True, 1).otherwise(0).alias("cancer_present"),
                               "atrophy_present")

patient_df = patient_df.select("*").filter("(mg_dl_t1 <> -2.0 OR mg_dl_t1 IS NULL) AND (mg_dl_t2 <> -2.0 OR mg_dl_t2 IS NULL) AND (mg_dl_t3 <> -2.0 OR mg_dl_t3 IS NULL)")

# Add Glucose Mean and round to 1 decimal point
patient_df = patient_df.withColumn("glucose_mean", round((col("mg_dl_t1") + col("mg_dl_t2") + col("mg_dl_t3"))/3, 1))

# Add Glucose Level category
patient_df = patient_df.withColumn("glucose_level", 
                                   when(patient_df.glucose_mean < 140, "normal"). \
                                   when((patient_df.glucose_mean > 140) & (patient_df.glucose_mean < 199), "prediabetes"). \
                                   when(patient_df.glucose_mean > 200, "diabetes"). \
                                   otherwise("not applicable"))

# Add date
patient_df = patient_df.withColumn("measurement_date", lit(current_date))

# Split patient_df into separate dataframes (one without missed readings and one with missed readings)
patient_reading_all_df = patient_df.select("*").filter("mg_dl_t1 IS NOT NULL AND mg_dl_t2 IS NOT NULL AND mg_dl_t3 IS NOT NULL")
patient_reading_null_df = patient_df.select("*").filter("mg_dl_t1 IS NULL OR mg_dl_t2 IS NULL OR mg_dl_t3 IS NULL")

# Output
patient_reading_all_df.write.format("json").mode("overwrite").save(f"processed/{current_date}_without_missed_readings")
patient_reading_null_df.write.format("json").mode("overwrite").save(f"processed/{current_date}_with_missed_readings")