# Databricks notebook source
#Generates a daily payment report by aggregating Gold Order Payment Summary data, including order count, total payments, refunds, outstanding balance, and net retained amount.
Silver_order=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_orders/")
Silver_payments=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_payments/")
Silver_refund=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_refund/")
display(Silver_order)
display(Silver_payments)
display(Silver_refund)

# COMMAND ----------

#Aggregating total order count and total order amount for each order date.
from pyspark.sql.functions import col,count,sum,countDistinct
Daily_order_df=Silver_order.groupBy("order_date").agg(countDistinct("order_id").alias("total_orders"),
                                                      sum("order_amount").alias("total_order_amount"))
display(Daily_order_df)

# COMMAND ----------

#Aggregating successful payment amounts and transaction counts by payment date.
from pyspark.sql.functions import col,sum,count
Daily_payment_df=Silver_payments.groupBy("payment_date").agg(sum("payment_amount")\
                                                             .alias("total_payment_amount"),
                                                             count("payment_id").alias("total_payments"))
display(Daily_payment_df)


# COMMAND ----------

##Aggregating successful refund amounts and refund counts by refund date.
Daily_refund_status_df=Silver_refund.filter(col("refund_status")=="SUCCESS")\
                                            .groupBy("refund_date").agg(sum("refund_amount")\
                                                                .alias("total_refund_amount"),
                                                                count("refund_id").alias("total_refunds"))
display(Daily_refund_status_df)
                                                        

# COMMAND ----------

#Joining daily Orders, Payments, and Refunds summaries using the business date.
Daily_Payments_Report_df=Daily_order_df.join(Daily_payment_df,Daily_order_df.order_date==Daily_payment_df.payment_date,"left")\
    .join(Daily_refund_status_df,Daily_order_df.order_date==Daily_refund_status_df.refund_date,"left")
display(Daily_Payments_Report_df)


# COMMAND ----------

# Calculate daily outstanding balance and net retained amount
from pyspark.sql.functions import col
Daily_Payments_Report_df=Daily_Payments_Report_df.withColumn("outstanding_balance",col("total_order_amount")-col("total_payment_amount"))\
    .withColumn("net_retined_amount",col("total_payment_amount")-col("total_refund_amount"))
display(Daily_Payments_Report_df)

# COMMAND ----------

#replace Daily Null values with o
from pyspark.sql.functions import col
Daily_Payments_Report_df=Daily_Payments_Report_df.fillna(0)
display(Daily_Payments_Report_df)

# COMMAND ----------

Final_Daily_Payment_Report=Daily_Payments_Report_df.select(
    col("order_date").alias("date"),
    col("total_orders"),
    col("total_order_amount"),
    col("total_payment_amount"),
    col("total_payments"),
    col("total_refund_amount"),
    col("total_refunds"),
    col("outstanding_balance"),
    col("net_retined_amount")
)
display(Final_Daily_Payment_Report)

# COMMAND ----------

# Create temporary view for the Gold daily payment report
Final_Daily_Payment_Report.createOrReplaceTempView("Daily_Payment_Reports")

# COMMAND ----------

# MAGIC %sql
# MAGIC -------Creating the Gold table for the daily payment summary.
# MAGIC CREATE OR REPLACE TABLE online_shop_catalog.gold.gold_daily_payment_summary
# MAGIC (
# MAGIC     order_date DATE,
# MAGIC     total_orders BIGINT,
# MAGIC     total_order_amount DOUBLE,
# MAGIC     total_payment_amount DOUBLE,
# MAGIC     total_payments BIGINT,
# MAGIC     total_refund_amount DOUBLE,
# MAGIC     total_refunds BIGINT,
# MAGIC     outstanding_balance DOUBLE,
# MAGIC     net_retained_amount DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC insert into online_shop_catalog.gold.gold_daily_payment_summary
# MAGIC select*from Daily_Payment_Reports;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from online_shop_catalog.gold.gold_daily_payment_summary;

# COMMAND ----------

# Load Gold Daily Payment Summary table into Gold Volume
Final_Daily_Payment_Report=spark.read.table("online_shop_catalog.gold.gold_daily_payment_summary")
Final_Daily_Payment_Report.write.mode("overwrite").format("csv").save("/Volumes/online_shop_catalog/gold/gold_volume/Gold_Daily_Payment_Summary",header=True)

