# Databricks notebook source
#Reads raw orders data from the Landing Volume and ingests it into the Bronze layer as a Delta table without applying business transformations.
orders_df=spark.read.format("csv").option("header","true").load("/Volumes/online_shop_catalog/landing/landing_volume/Source_Files/orders.csv")
display(orders_df)

# COMMAND ----------

# Creates a temporary SQL view from the orders_df DataFrame to query and process raw Orders data using Spark SQL before loading it into the Bronze layer.
orders_df.createOrReplaceTempView("orders")
spark.sql("select*from orders").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- Creating Bronze layer orders table
# MAGIC create or replace  table  online_shop_catalog.bronze.bronze_orders(
# MAGIC     order_id string,
# MAGIC     customer_id string,
# MAGIC     order_date string,
# MAGIC     order_amount double,
# MAGIC     order_status string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC ------Copies raw orders data from the orders temp view into the Bronze layer bronze_orders Delta table without applying any transformations.
# MAGIC insert into online_shop_catalog.bronze.bronze_orders   select
# MAGIC order_id,
# MAGIC customer_id,
# MAGIC order_date,
# MAGIC order_amount,
# MAGIC order_status
# MAGIC from orders;
# MAGIC

# COMMAND ----------

#Stores the ingested Bronze Orders data in the Bronze Volume for data persistence, backup, and future processing
Bronze_orders_df=spark.read.table("online_shop_catalog.bronze.bronze_orders")
display(Bronze_orders_df)
Bronze_orders_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_orders",header=True)