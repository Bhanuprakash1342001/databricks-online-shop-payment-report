# Databricks notebook source
##Reads the Payments data stored in Delta format from the Bronze Volume into a PySpark DataFrame for validation, cleansing, deduplication, and loading into the Silver layer.
Payments_df=spark.read.format("delta").option("header","true").load("/Volumes/online_shop_catalog/bronze/bronze_volume/bronze_payments/")
display(Payments_df)

# COMMAND ----------

#Payments business rule validation
#BR01	Payment ID must not be NULL	Reject records without Payment ID
##BR02	Order ID must not be NULL	Reject records without Order ID
#BR03	Payment date must not be NULL	Reject records without Payment Date
from pyspark.sql.functions import col
Payment_Valid_df=Payments_df.filter((col("order_id").isNotNull())&
                                    (col("payment_id").isNotNull())&
                                    (col("payment_amount").isNotNull())&
                                    (col("payment_method").isNotNull())&
                                    (col("payment_status").isNotNull())&
                                    (col("payment_date").isNotNull()))
display(Payment_Valid_df)


# COMMAND ----------

###BR04	Payment amount must be greater than zero	Reject zero or negative amounts
Payment_ValidAmount_df=Payment_Valid_df.filter(col("payment_amount")>0)
display(Payment_ValidAmount_df)


# COMMAND ----------

#BR05	Payment status must be valid	Retain valid statuses such as SUCCESS, FAILED, PENDING
Payment_ValidStatus_df=Payment_ValidAmount_df.filter(col("payment_status").isin("SUCCESS","FAILED","PENDING"))
display(Payment_ValidStatus_df)


# COMMAND ----------

##BR06	Remove duplicate Payment IDs	Same payment uploaded twice should count only once
Payment_Clean_df=Payment_ValidStatus_df.dropDuplicates(["payment_id"])
display(Payment_Clean_df)
#Note:this code removes duplicate records based on payment_id and keeps only one record per unique Payment id.


# COMMAND ----------

##BR07	Allow multiple payments for one Order ID	Two ₹500 payments for one order should total ₹1,000##
from pyspark.sql.functions import sum, count

Payments_Order_Summary_df = Payment_Clean_df.groupBy("order_id").agg(
    sum("payment_amount").alias("Total_Payment_Amount"),
    count("payment_id").alias("Total_Payments")
)

display(Payments_Order_Summary_df)

# COMMAND ----------

#Filter O1001
from pyspark.sql.functions import col
O1001_Payment_df=Payment_Clean_df.filter(col("order_id") == "O1001")\
    .agg(sum("payment_amount").alias("Total_Payment_Amount"))
display(O1001_Payment_df)
    

# COMMAND ----------

#creating temp views for Payments cleaned and summary DataFrames
Payment_Clean_df.createOrReplaceTempView("Payments_Cleaned")
Payments_Order_Summary_df.createOrReplaceTempView("Order_Summary")
O1001_Payment_df.createOrReplaceTempView("O1001_Payments")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ------Creates the Silver Payments Delta table by loading all validated, cleansed, and deduplicated payment records from the temporary view while preserving all six original columns.
# MAGIC create or replace table online_shop_catalog.silver.Silver_payments(
# MAGIC payment_id string,
# MAGIC order_id string,
# MAGIC payment_date string,
# MAGIC payment_amount double,
# MAGIC payment_status string,
# MAGIC payment_method string
# MAGIC
# MAGIC )using delta;
# MAGIC
# MAGIC insert into online_shop_catalog.silver.silver_payments  select *from Payments_Cleaned;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ----Creates the Silver Payments Summary Delta table with order-wise aggregated payment information, including Order ID, total payment amount, and total number of payment transactions. This table supports payment reconciliation and Gold-layer reporting.
# MAGIC create or replace table online_shop_catalog.silver.silver_payment_summary(
# MAGIC order_id string,
# MAGIC total_payment_amount double,
# MAGIC total_payment long
# MAGIC )using delta;
# MAGIC insert into online_shop_catalog.silver.silver_payment_summary select*from Order_Summary;

# COMMAND ----------

#Loads all validated, cleaned, and deduplicated Payments records into the Silver Volume in Delta format.
Payments_df=spark.read.table("online_shop_catalog.silver.silver_payments ")
Payments_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/silver/silver_volume/silver_payments",header=True)

# COMMAND ----------

##Loads order-wise aggregated payment details, including total payment amount and transaction count, into the Silver Volume in Delta format
Payments_Order_Summary_df=spark.read.table("online_shop_catalog.silver.silver_payment_summary ")
Payments_Order_Summary_df.write.mode("overwrite").format("delta").save("/Volumes/online_shop_catalog/silver/silver_volume/silver_payments_summary",header=True)