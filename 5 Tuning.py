# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Tuning on Databricks
# MAGIC </br>
# MAGIC Use a <a href='https://docs.databricks.com/en/clusters/photon.html'>Photon cluster</a> to see the full performance improvements outlined in this notebook
# MAGIC </br>
# MAGIC Use Databricks Runtime 13.3 LTS or higher to take advantage of the latest performance tuning features

# COMMAND ----------

# DBTITLE 1,Install the Library
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Run setup
from utils.onboarding_setup import get_config, iot_data_generator, reset_tables
config = get_config(spark)
reset_tables(spark, config, dbutils)

# COMMAND ----------

# DBTITLE 1,Create test ETL
def test_etl(target):
  ''' 
  This function runs a test ETL process against the specified target. 
  WARNING: This intentionally uses bad practices for demonstration, do not copy!!! 
  '''
  iot_data_generator(spark, 1).write.mode('append').saveAsTable(target)
  for i in range(3):
    tuning_test_data = iot_data_generator(spark, 10000).dropDuplicates(['device_id']).createOrReplaceTempView("new_data")
    spark.sql(f'''
          MERGE INTO {target} t
          USING new_data s
          ON s.device_id = t.device_id
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
    ''')
  iot_data_generator(spark, 1000000).write.mode('append').option('maxRecordsPerFile', '5000').saveAsTable(target)
# REPEAT - BAD PRACTICES IN THIS CELL FOR TUNING DEMO PURPOSES ONLY. DO NOT REPLICATE!!!

# COMMAND ----------

# DBTITLE 1,Create a test query
def test_query(source_table):
  ''' This function is a nonsensical "no operation" query intended to test performance against a source table '''
  df = spark.read.table(source_table)
  agg_df = df.where('temperature > 220 or temperature < 185').groupby('device_id', 'option_number').count()
  joined_df = agg_df.join(df, 'device_id')
  joined_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the normal table

# COMMAND ----------

# DBTITLE 1,Time the test ETL
# MAGIC %timeit test_etl(config['bronze_table'])  # Time the etl to the regular table. This may take a few minutes

# COMMAND ----------

# DBTITLE 1,Clear the cache for testing
# MAGIC %sql CLEAR CACHE

# COMMAND ----------

# DBTITLE 1,Time the test query
# MAGIC %timeit test_query(config['bronze_table'])  # Time the the query on the regular table. This may take a couple of minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the tuned table

# COMMAND ----------

# DBTITLE 1,Create tuned table
# To improve performance, we can make changes to the layout of our data. Let's create a tuned aggregate table
spark.sql(f'''
  CREATE OR REPLACE TABLE {config['tuned_bronze_table']} 
  ( device_id STRING, timestamp TIMESTAMP, factory_id STRING, 
    option_number BIGINT, model_id STRING, rotation_speed DOUBLE, pressure DOUBLE, 
    temperature DOUBLE, airflow_rate DOUBLE, delay DOUBLE, density DOUBLE )
  CLUSTER BY (device_id, temperature, factory_id) -- By using clustering keys we speed up queries that use those columns
''')
spark.sql(f''' -- By turning on deletion vectors we speed up merges, updates, deletes, and "needle in the haystack" lookups
  ALTER TABLE {config['tuned_bronze_table']} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
''')

# COMMAND ----------

# DBTITLE 1,Time the test ETL
# MAGIC %timeit test_etl(config['tuned_bronze_table'])  # Time the etl to the tuned table. This may take a few minutes

# COMMAND ----------

# DBTITLE 1,Compact small files
spark.sql(f''' -- By running OPTIMIZE we compact small files which speeds up reads
  OPTIMIZE {config['tuned_bronze_table']}; 
''')
spark.sql(f''' -- By running analyze table we compute statistics which speed up reads
  ANALYZE TABLE {config['tuned_bronze_table']} COMPUTE STATISTICS FOR ALL COLUMNS; 
''')

# COMMAND ----------

# DBTITLE 1,Clear the cache for testing
# MAGIC %sql CLEAR CACHE

# COMMAND ----------

# DBTITLE 1,Time the test query
# MAGIC %timeit test_query(config['tuned_bronze_table']) # Time the querying the tuned table. This may take a minute

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove old, redundant files from our storage

# COMMAND ----------

# DBTITLE 1,Show the number of files in the table
# The repeated writes, merges, and bad practices in etl_test left us with a lot of files
table_folder = spark.sql(f"DESCRIBE DETAIL {config['tuned_bronze_table']}").collect()[0]['location']
display(dbutils.fs.ls(table_folder))

# COMMAND ----------

# DBTITLE 1,Remove old files
spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')
# Remove old metadata
spark.sql(f''' 
    REORG TABLE {config['tuned_bronze_table']} APPLY (PURGE)
''')
# Remove old files
spark.sql(f'''
    VACUUM {config['tuned_bronze_table']} RETAIN 0 HOURS
''')
display(dbutils.fs.ls(table_folder)) # ... way fewer files!

# COMMAND ----------

# DBTITLE 1,To drop the demo tables, uncomment and run this cell
# reset_tables(spark, config, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC To learn more about performance tuning on Databricks, check out our documentation for
# MAGIC <a href="https://docs.databricks.com/en/delta/tune-file-size.html#what-is-auto-optimize-on-databricks">auto optimize</a>,
# MAGIC <a href="https://docs.databricks.com/en/delta/deletion-vectors.html">deletion vectors</a>, and <a href="https://docs.databricks.com/en/lakehouse-architecture/performance-efficiency/best-practices.html">efficiency best practices</a>
