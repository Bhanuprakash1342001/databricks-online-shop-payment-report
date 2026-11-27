# Databricks notebook source
#Reads raw Refunds data from the Landing Volume and loads it into the Bronze layer for further processing.
Refund_df=spark.read.format("csv").option("header","true").load("/Volumes/online_shop_catalog/landing/landing_volume/Source_Files/refunds.csv",inferSchema="true")
display(Refund_df)


# COMMAND ----------

##Creates a temporary SQL view from the refunds_df DataFrame to query and process raw Refunds data using Spark SQL before loading it into the Bronze layer.
Refund_df.createOrReplaceTempView("Refund")
spark.sql("select*from Refund").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC ----creating bronze_refund table in the bronze layer
# MAGIC create  or replace table online_shop_catalog.bronze.bronze_refund(
# MAGIC     refund_id string,
# MAGIC     order_id string,
# MAGIC     payment_id string,
# MAGIC     refund_date string,
# MAGIC     received_date string,
# MAGIC     refund_amount double,
# MAGIC     refund_status string,
# MAGIC     refund_reason string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -----Stores the raw refund transaction amount received from the source system in the Bronze Refunds table without applying any transformations.
# MAGIC insert into online_shop_catalog.bronze.bronze_refund
# MAGIC select 
# MAGIC refund_id,
# MAGIC order_id,
# MAGIC payment_id,
# MAGIC refund_date,
# MAGIC received_date,
# MAGIC refund_amount,
# MAGIC refund_status,
# MAGIC refund_reason
# MAGIC from Refund;
# MAGIC

# COMMAND ----------

##Stores the ingested raw Refunds data in Delta format for persistence and future processing.
Bronze_Refund_df=spark.read.table("online_shop_catalog.bronze.bronze_refund")
display(Bronze_Refund_df)
Bronze_Refund_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_refund",header=True,InferSchema=True)