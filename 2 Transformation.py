# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Transformation in Databricks</h1>

# COMMAND ----------

# DBTITLE 1,Install Data Generation Library
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Import demo library and get configuration
from utils.onboarding_setup import get_config, defect_data_generator
from pyspark.sql.functions import *
config = get_config(spark)
iot_data = spark.read.table(config['bronze_table'])
defect_data = defect_data_generator(spark, iot_data)
defect_data.write.mode('overwrite').saveAsTable(config['defect_table'])

# COMMAND ----------

# DBTITLE 1,Display the data we'll be working with
iot_data.display()

# COMMAND ----------

# DBTITLE 1,Add some calculations to the data
from pyspark.sql.functions import *      # import spark's functions to use for transformations

transformed_df = iot_data.withColumn('heatingRate', col('temperature') / col('heatingTime')) # caluclate a new heatingRate column
transformed_df.display()

# COMMAND ----------

# DBTITLE 1,Chain multiple transformations together
multi_transformed_df = (
  transformed_df
  .withColumnRenamed('temperature', 'temperatureFahrenheit') # Rename a column
  .withColumn('heatingRateSqrt', sqrt('heatingRate')) # Take the square root 
  .where('injectionID is not null')
  .where('campaignID is not null')
  .withColumn('compositeID', concat('injectionID', lit(':'), 'campaignID')) # Concatenate two columns and a literal
  .dropDuplicates(['compositeID', 'timestamp']) # Drop rows with duplicate compositeID and timestamp columns
  .drop('_rescued_data') # Remove the rescued data column from our schema inference step during ingestion
)
multi_transformed_df.display()

# COMMAND ----------

# DBTITLE 1,Join in our defect data
defect_df = spark.read.table(config['defect_table']) # get the defects column for reporting or train predictive models
joined_df = multi_transformed_df.join(defect_df, ['timestamp', 'injectionID'], 'left') # left join on timestamp and injectionID
joined_df.write.mode('overwrite').saveAsTable(config['silver_table']) # write to the silver table

# COMMAND ----------

# DBTITLE 1,Aggregate and get a count of defects
silver_df = spark.read.table(config['silver_table'])
summed_df = silver_df.groupBy('defect').count()
summed_df.write.mode('overwrite').saveAsTable(config['gold_table'])
spark.read.table(config['gold_table']).display()

# COMMAND ----------


