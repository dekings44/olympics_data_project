# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "6c4df172-705d-4f38-8898-9ca78c065caa",
"fs.azure.account.oauth2.client.secret": 'WMi8Q~NCwkss-eYopiK6UaBCRAcu~XECN-mrDc5-',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/28f96518-fbea-412f-bf53-50e8c138c7fd/oauth2/token"}



dbutils.fs.mount(
source = "abfss://tokoydata@tokyoolympicsproject.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/olymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olymic"

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load("/mnt/olymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/olymic/raw-data/coaches.csv")
gender = spark.read.format("csv").option("header","true").load("/mnt/olymic/raw-data/gender.csv")
medal = spark.read.format("csv").option("header","true").load("/mnt/olymic/raw-data/medal.csv")
team = spark.read.format("csv").option("header","true").load("/mnt/olymic/raw-data/team.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

gender.show()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender = gender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

medal.show()

# COMMAND ----------

medal.printSchema()

# COMMAND ----------

medal = medal.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))