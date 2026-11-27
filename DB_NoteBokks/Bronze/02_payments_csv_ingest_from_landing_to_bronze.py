# Databricks notebook source
##Reads raw Payments data from the Landing Volume and loads it into the Bronze layer as a Delta table without applying business transformations.
Payments_df=spark.read.format("csv").option("header","true").load("/Volumes/online_shop_catalog/landing/landing_volume/Source_Files/payments.csv")
display(Payments_df)

# COMMAND ----------

#Creates a temporary SQL view from the payments_df DataFrame to query and process raw Payments data using Spark SQL before loading it into the Bronze layer.
Payments_df.createOrReplaceTempView("Payments")
spark.sql("select*from Payments").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC ---now creating a bronze layer payments table
# MAGIC create or replace table online_shop_catalog.bronze.bronze_payments(
# MAGIC     payment_id string,
# MAGIC     order_id string,
# MAGIC     payment_date string,
# MAGIC     payment_amount double,
# MAGIC     payment_status string,
# MAGIC     payment_method string
# MAGIC )using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC ------Copies raw payments data from the payments temp view into the Bronze layer bronze_payments Delta table without applying any transformations.
# MAGIC insert into online_shop_catalog.bronze.bronze_payments 
# MAGIC select 
# MAGIC payment_id,
# MAGIC order_id,
# MAGIC payment_date,
# MAGIC payment_amount,
# MAGIC payment_status,
# MAGIC payment_method  from Payments;

# COMMAND ----------

#Stores the ingested Bronze payments data in the Bronze Volume for data persistence, backup, and future processing
Bronze_Payments_df=spark.read.table("online_shop_catalog.bronze.bronze_payments ")
display(Bronze_Payments_df)
Bronze_Payments_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_payments",header=True)