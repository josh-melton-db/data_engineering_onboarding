# Databricks notebook source
# MAGIC %md
# MAGIC # CI/CD on Databricks

# COMMAND ----------

# DBTITLE 1,Install data generator
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Run setup
from utils.onboarding_setup import get_config
from pyspark.sql.functions import isnull, col
config = get_config(spark)

# COMMAND ----------

# DBTITLE 1,First define your code in functions
def transform_df(input_df):
  output_df = (
    input_df
    .where('injectionID is not null')
    .where('campaignID is not null')
    .dropDuplicates(['injectionID', 'campaignID'])
  )
  return output_df

# COMMAND ----------

# DBTITLE 1,Next set up your data to test
df = spark.read.table(config['bronze_table'])
test_df = transform_df(df)

# COMMAND ----------

# DBTITLE 1,Run tests
null_count = test_df.where(isnull(col('injectionID'))).count()
duplicate_count = test_df.groupBy('injectionID', 'campaignID').count().where('count > 1').count()

assert null_count == 0
assert duplicate_count == 0

# COMMAND ----------

# MAGIC %md
# MAGIC This was a limited introduction to writing unit tests in Databricks. For more detail on unit testing in different scenarios, try <a ref="https://docs.databricks.com/en/notebooks/testing.html">testing on Databricks</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Follow our <a href="https://docs.databricks.com/en/repos/git-operations-with-repos.html">repos instructions</a> to see the git operations supported in Databricks Repos

# COMMAND ----------

# MAGIC %md
# MAGIC You can also clone a repo locally and do your Databricks development in an IDE. To learn more about developing in IDEs, follow our documentation for <a href="https://docs.databricks.com/en/dev-tools/index-ide.html"> connecting with your favorite IDE</a>

# COMMAND ----------

# MAGIC %md
# MAGIC You can develop and deploy code and other Databricks assets using Databricks Asset Bundles, like shown in our <a href="https://www.databricks.com/resources/demos/tours/data-engineering/databricks-asset-bundles?itm_data=demo_center">Demo Center walkthrough</a>. To see the workflow that you configured in the last notebook as an Asset Bundle, click the three dots in the top right and select "View YML/JSON"
# MAGIC <img style="float: right" width="800" src="https://github.com/josh-melton-db/data_engineering_onboarding/blob/main/utils/images/view_yml.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, you can tie all these concepts into your standard CI/CD processes like shown in <a href="https://docs.databricks.com/en/dev-tools/bundles/ci-cd.html">Databricks CI/CD workflow</a>
