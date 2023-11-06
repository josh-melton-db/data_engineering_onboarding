# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Orchestration in Databricks</h1>

# COMMAND ----------

# MAGIC %md
# MAGIC In the menu on the left hand side, select "Workflows" </br>
# MAGIC <img style="float: right" width="200" src="https://github.com/josh-melton-db/data_engineering_onboarding/blob/main/utils/images/workflows.png?raw=true">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC In the top right, click "Create Job" </br>
# MAGIC <img style="float: right" width="200" src="https://github.com/josh-melton-db/data_engineering_onboarding/blob/main/utils/images/create_job.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC In the menu that opens add your task name, select the "1 Data Ingestion" notebook under "path", and select a cluster. Interactive clusters stay on and are great for development (that's what you've been using in the notebooks), but jobs clusters are significantly cheaper (best for scheduled production jobs). Once you're done configuring the task, click "Create"  </br>
# MAGIC <img style="float: right" width="800" src="https://github.com/josh-melton-db/data_engineering_onboarding/blob/main/utils/images/create_task.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC After creating your first task, add another task by clicking "Add Task", select "notebook" as the type, and configure the second task to use the "2 Transformation" notebook with the same cluster as the first task. Make sure the Transformation task depends on the Ingestion task

# COMMAND ----------

# MAGIC %md
# MAGIC Click "Run Now" to run the job. Click "Add Trigger" to see options for scheduling the job. Note that you can also set job and task parameters, add if/then or try/catch logic in your workflows, and more
