# Databricks notebook source
##Reads raw Orders data from the Bronze layer, applies data quality checks and business rules, and loads validated, deduplicated records into the Silver Orders Delta table.
Orders_df=spark.read.format("delta").option("header","true").load("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_orders/")
display(Orders_df)

# COMMAND ----------

#(Business rule comments for your notebook
#1. BR01  Validates mandatory Order ID.
#2. BR02  Validates mandatory Customer ID.
#3. BR03  Validates mandatory Order Date.
#4. BR04 Validates mandatory Order Amount.
#5. BR05  Rejects zero or negative Order Amount.


# COMMAND ----------

#Applies six business rules to the Bronze Orders data to validate mandatory fields, check positive order amounts, identify invalid records, and remove duplicate Order IDs before loading clean data into the Silver Orders table.
from pyspark.sql.functions import col
Orders_Valid_df=Orders_df.filter((col("order_id").isNotNull())&
                                 (col("customer_id").isNotNull())&
                                 (col("order_date").isNotNull()) &
                                 (col("order_amount").isNotNull()) &
                                 (col("order_amount")>0))
display(Orders_Valid_df)


# COMMAND ----------

## BR06 – Removes duplicate records based on Order ID).
from pyspark.sql.functions import col
Orders_Clean_df=Orders_Valid_df.dropDuplicates(['order_id'])
display(Orders_Valid_df)

# COMMAND ----------

#check the Recount count of all 3 dataframes
Orders_count = Orders_df.count()
Valid_count = Orders_Valid_df.count()
Clean_count = Orders_Clean_df.count()

rejected_records = Orders_count - Valid_count
duplicate_records_removed = Valid_count - Clean_count

print("Bronze Orders Count:", Orders_count)
print("Valid Orders Count:", Valid_count)
print("Clean Orders Count:", Clean_count)
print("Rejected Records:", rejected_records)
print("Duplicate Records Removed:", duplicate_records_removed)

# COMMAND ----------

#Creates a temporary SQL view from the validated and deduplicated Orders DataFrame to query and load clean order records into the Silver layer using Spark SQL.
Orders_Clean_df.createOrReplaceTempView("Orders")
spark.sql("select*from Orders").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC ----Creates the Silver Orders Delta table with all five columns from the validated and deduplicated temporary view, preserving complete order information for further processing and daily payment reporting.
# MAGIC create or replace table online_shop_catalog.silver.silver_orders(
# MAGIC     order_id string,
# MAGIC     customer_id string,
# MAGIC     order_date string,
# MAGIC     order_amount double,
# MAGIC     order_status string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Loads all validated, cleaned, and deduplicated Orders records from the temporary view into the Silver Orders Delta table for downstream reporting and analysis.
# MAGIC insert into online_shop_catalog.silver.silver_orders  select
# MAGIC order_id,
# MAGIC customer_id,
# MAGIC order_date,
# MAGIC order_amount,
# MAGIC order_status from Orders;

# COMMAND ----------

#Stores the cleaned and validated Silver Orders data in Delta format for data persistence, future transformations, and Gold-layer reporting.
Silver_Order_df=spark.read.table("online_shop_catalog.silver.silver_orders")
display(Silver_Order_df)
Silver_Order_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/silver/silver_volume/silver_orders",header=True,InferSchema=True)