# Databricks notebook source
#Reads the Bronze Refunds Delta data from the Bronze Volume for validation and cleansing.
Refund_df=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_refund/")
display(Refund_df)


# COMMAND ----------

# Filters Refunds records by removing rows with NULL values in mandatory columns.
from pyspark.sql.functions import col
Refund_Valid_df=Refund_df.filter((col("order_id").isNotNull())&
                                 (col("payment_id").isNotNull())&
                                 (col("refund_date").isNotNull())&
                                 (col("received_date").isNotNull())&
                                 (col("refund_amount").isNotNull())&
                                 (col("refund_id").isNotNull())&
                                 (col("refund_reason").isNotNull())&
                                 (col("refund_status").isNotNull()))
display(Refund_Valid_df)

# COMMAND ----------

##Keeps only Refunds records with a refund amount greater than zero
Refund_Amount_df=Refund_Valid_df.filter(col("refund_amount")>0)
display(Refund_Amount_df)

# COMMAND ----------

##Keeps Refunds records with valid refund statuses.
Refund_Status_df=Refund_Amount_df.filter(col("refund_status").isin("SUCCESS", "PENDING", "FAILED"))
display(Refund_Status_df)

# COMMAND ----------

##Removes duplicate Refund records based on refund_id and keeps one record per unique Refund ID.
Refund_Clean_df=Refund_Status_df.dropDuplicates(["refund_id"])
display(Refund_Clean_df)

# COMMAND ----------

#record count checking
bronze_count=Refund_df.count()
valid_count=Refund_Valid_df.count()
amount_count=Refund_Amount_df.count()
status_count=Refund_Status_df.count()
clean_count=Refund_Clean_df.count()
print("Bronze Refund count:", bronze_count)
print("Valid Refund count:", valid_count)
print("Refund Amount count:", amount_count)
print("Refund Status count:", status_count)
print("Refund Clean count:",clean_count)

# COMMAND ----------



# COMMAND ----------

#Creates a temporary SQL view from the validated and deduplicated Refunds DataFrame for loading into the Silver layer.
Refund_Clean_df.createOrReplaceTempView("Refund")

# COMMAND ----------

# MAGIC %sql
# MAGIC ----creates the Silver Refunds Delta table with all 8 columns from the cleaned Refunds data.
# MAGIC create or replace table online_shop_catalog.silver.silver_Refund(
# MAGIC     refund_id string,
# MAGIC     order_id string,
# MAGIC     payment_id string,
# MAGIC     refund_date string,
# MAGIC     received_date string,
# MAGIC     refund_amount double,
# MAGIC     refund_status string,
# MAGIC     refund_reason string
# MAGIC ) using delta;
# MAGIC insert into online_shop_catalog.silver.silver_refund select*from Refund;

# COMMAND ----------

#Stores the cleaned Silver Refunds data in Delta format in the Silver Volume.
Silver_Refund_df=spark.read.table(" online_shop_catalog.silver.silver_refund")
Silver_Refund_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/silver/silver_volume/silver_refund",header=True)