# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Reading Data in Databricks</h1>

# COMMAND ----------

# DBTITLE 1,Install the data generation library
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Import the demo library
from utils.onboarding_setup import get_config, reset_tables, iot_data_generator

# COMMAND ----------

# DBTITLE 1,Set up the demo
config = get_config(spark)
reset_tables(spark, config, dbutils)
iot_data = iot_data_generator(spark, config['rows_per_run'])
iot_data.write.mode('overwrite').saveAsTable(config['bronze_table'])

# COMMAND ----------

# DBTITLE 1,Read the bronze table's data and display a count
# Click the + icon next to the "Table" label below and generate a plot of the count of injectors (injectorIDs) over waterFlowRate
bronze_df = spark.read.table(config['bronze_table'])
bronze_df.display()

# COMMAND ----------

# DBTITLE 1,Generate more data and write it to the bronze table
data = iot_data_generator(spark, config['rows_per_run']) # generate more data
data.write.mode('append').saveAsTable(config['bronze_table']) # write to bronze

# COMMAND ----------

# DBTITLE 1,Visualize data, note totals
# Create the same visual as before and notice that the totals encompass the entire table, including rows we've seen before
new_bronze_df = spark.read.table(config['bronze_table'])
new_bronze_df.display()

# COMMAND ----------

# DBTITLE 1,Simulate external CSV data landing in storage
config = get_config(spark)
data = iot_data_generator(spark, config['rows_per_run']) # generate more data
data.write.option('header', 'true').mode('overwrite').csv(config['csv_staging']) # land the data as csv files

# COMMAND ----------

# DBTITLE 1,Read new data landing in cloud storage, write to bronze table
bronze_incremental_df = (
  spark.readStream.format('cloudFiles') # load in new data from cloud storage
  .option("cloudFiles.format", "csv") # csv files
  .option("cloudFiles.schemaHints", "timestamp TIMESTAMP, option_number LONG, rotation_speed DOUBLE, pressure DOUBLE, temperature DOUBLE, airflow_rate DOUBLE, delay DOUBLE, density DOUBLE") # give schema hints if known, otherwise defaults to strings
  .option('cloudFiles.schemaLocation', config['checkpoint_location']) # infer the rest of the schema and store in the checkpoint
  .option('header', 'true') # headers are expected
  .load(config['csv_staging']) # path to the data
)

(
  bronze_incremental_df.writeStream
  .option('checkpointLocation', config['checkpoint_location'])
  .option("mergeSchema", "true")
  .outputMode('append')
  # .trigger(availableNow=True)   # Uncommenting this line would "schedule" the ingestion stream instead of running real time
  .toTable(config['bronze_table'])
  # .awaitTermination()
)

# COMMAND ----------

bronze_df = spark.read.table(config['bronze_table'])
print('Bronze rows: ', bronze_df.count())
bronze_df.display()

# COMMAND ----------

# DBTITLE 1,Generate more data, watch the totals change
config = get_config(spark)
data = iot_data_generator(spark, config['rows_per_run'])
data.write.option('header', 'true').mode('append').csv(config['csv_staging'])

# COMMAND ----------

from time import sleep
sleep(15) # Wait for newly written files to be picked up
bronze_df = spark.read.table(config['bronze_table'])
print('Bronze rows: ', bronze_df.count())
bronze_df.display()

# COMMAND ----------

for stream in spark.streams.active:
  stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC To try this with your own flat file, try the Data Ingestion UI in the Data Engineering menu on the left

# COMMAND ----------


