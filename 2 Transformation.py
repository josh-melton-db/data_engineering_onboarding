# Databricks notebook source
# MAGIC %md
# MAGIC #Transformation in Databricks
# MAGIC </br>
# MAGIC This notebook assumes you've already completed notebook 1 Data Ingestion

# COMMAND ----------

# DBTITLE 1,Install Data Generation Library
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Import demo library and get configuration
from utils.onboarding_setup import get_config, defect_data_generator
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

transformed_df = iot_data.withColumn('heatingRate', col('temperature') / col('delay')) # caluclate a new heatingRate column
transformed_df.display()

# COMMAND ----------

# DBTITLE 1,Chain multiple transformations together
multi_transformed_df = (
  transformed_df
  .withColumnRenamed('temperature', 'temperatureFahrenheit') # Rename a column
  .withColumn('delay_sqrt', sqrt('delay')) # Take the square root 
  .where('device_id is not null')
  .where('factory_id is not null')
  .withColumn('composite_id', concat('device_id', lit(':'), 'factory_id')) # Concatenate two columns and a literal
  .dropDuplicates(['composite_id', 'timestamp']) # Drop rows with duplicate compositeID + timestamps
  .drop('_rescued_data') # Remove the rescued data column from our schema inference step during ingestion
)
multi_transformed_df.display()

# COMMAND ----------

# DBTITLE 1,Join in our defect data
defect_df = spark.read.table(config['defect_table']) # get the defects column for reporting or train predictive models
joined_df = multi_transformed_df.join(defect_df, ['timestamp', 'device_id'], 'left') # left join on timestamp and device_id
joined_df.write.mode('overwrite').saveAsTable(config['silver_table']) # write to the silver table

# COMMAND ----------

# DBTITLE 1,Aggregate and get a count of defects
silver_df = spark.read.table(config['silver_table'])
summed_df = silver_df.groupBy('defect', 'factory_id').count() # Perform a group by on the defect column to get counts per factory
summed_df.write.mode('overwrite').saveAsTable(config['gold_table'])
spark.read.table(config['gold_table']).display()

# COMMAND ----------


