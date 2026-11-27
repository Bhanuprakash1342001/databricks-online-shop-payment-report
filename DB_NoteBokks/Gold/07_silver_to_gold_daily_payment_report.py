# Databricks notebook source
##Reads all cleaned and validated Orders, Payments, Payments Summary, and Refunds Delta datasets from the Silver Volume for Gold-layer processing.
Silver_order=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_orders/")
Silver_payments=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_payments/")
Silver_payment_Summary=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_payments_summary/")
Silver_refund=spark.read.format("delta").option("header","True").load("/Volumes/online_shop_catalog/silver/silver_volume/silver_refund/")
display(Silver_order)
display(Silver_payments)
display(Silver_payment_Summary)
display(Silver_refund)

# COMMAND ----------

#Check record counts
print("silver_order_count:",Silver_order.count())
print("silver_payment_count:",Silver_payments.count())
print("silver_payment_summary_count:",Silver_payment_Summary.count())
print("silver_refund_count:",Silver_refund.count())

# COMMAND ----------

##Only successful payment transactions should contribute to the total payment amount. Multiple payments for the same Order ID must be combined.
from pyspark.sql.functions import col,sum,count
Payment_orders_Gold_df=Silver_payments.filter(col("payment_status")=="SUCCESS").groupBy("order_id").agg(
    sum("payment_amount").alias("total_payment_amount"),
    count("payment_id").alias("total_payment")
)
display(Payment_orders_Gold_df)

# COMMAND ----------

##Only successful refunds should be included in the refund amount calculation. Multiple refunds for the same Order ID must be combined.
from pyspark.sql.functions import col
Refund_Orders_Gold_df=Silver_refund.filter(col("refund_status")=="SUCCESS").groupBy("order_id")\
    .agg(sum("refund_amount").alias("total_refund_amount"),
    count("refund_id").alias("total_refunds"))
display(Refund_Orders_Gold_df)

# COMMAND ----------

#Join Orders, Payments and Refunds
from pyspark.sql.functions import col,lit,coalesce
Order_Payment_df=Silver_order\
.join(Payment_orders_Gold_df,"order_id","left")\
.join(Refund_Orders_Gold_df,"order_id","left")\
    .fillna(0)
display(Order_Payment_df)


# COMMAND ----------

#Calculates outstanding balance and net retained amount for each order using successful payment and refund totals.
from pyspark.sql.functions import col
order_payment_gold_df=Order_Payment_df.withColumn("outstanding_balance",col("total_payment_amount"))\
    .withColumn("net_retained_amount",col("total_refund_amount"))
display(order_payment_gold_df)

# COMMAND ----------

df=order_payment_gold_df.filter(col("order_id").isin("O1001","O1002"))
display(df)

# COMMAND ----------

#Creates the Gold Order Payment Summary table containing order details, successful payment totals, refund totals, outstanding balance, and net retained amount.
order_payment_gold_df.createOrReplaceTempView("order_payment_gold")


# COMMAND ----------

# MAGIC %sql
# MAGIC ----gold_order_payment_summary
# MAGIC create or replace table online_shop_catalog.gold.gold_order_payment_summary(
# MAGIC     order_id string,
# MAGIC     customer_id string,
# MAGIC     order_date date,
# MAGIC     order_amount double,
# MAGIC     order_status string,
# MAGIC     total_payment_amount double,
# MAGIC     total_payment bigint,
# MAGIC     total_refund_amount double,
# MAGIC     total_refunds bigint,
# MAGIC     outstanding_balance double,
# MAGIC     net_retained_amount double
# MAGIC
# MAGIC )using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  online_shop_catalog.gold.gold_order_payment_summary
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     TO_DATE(order_date) AS order_date,
# MAGIC     order_amount,
# MAGIC     order_status,
# MAGIC     total_payment_amount,
# MAGIC     total_payment,
# MAGIC     total_refund_amount,
# MAGIC     total_refunds,
# MAGIC     outstanding_balance,
# MAGIC     net_retained_amount
# MAGIC FROM order_payment_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from online_shop_catalog.gold.gold_order_payment_summary;

# COMMAND ----------

#Stores the Gold Order Payment Summary data in Delta format in the Gold Volume for reporting and future analysis.
Gold_order_payment_summary_df=spark.read.table("online_shop_catalog.gold.gold_order_payment_summary")
Gold_order_payment_summary_df.write.mode("overwrite").format("csv").save("/Volumes/online_shop_catalog/gold/gold_volume/Gold_Order_Payment_Summary",header=True)