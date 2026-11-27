# Online Shop Daily Payment Report -- Databricks Project

## 1. Project Overview

This project demonstrates an end-to-end data engineering pipeline using
Databricks, PySpark, SQL, Delta Lake, Unity Catalog, and Databricks
Volumes.

The objective is to generate a daily payment report for an online
shopping business and answer:

-   How many orders were placed?
-   How much payment was received?
-   How much refund was issued?
-   How much is still outstanding?
-   How much payment was retained after refunds?

## 2. Technologies Used

-   Databricks
-   PySpark
-   Python
-   SQL
-   Delta Lake
-   Unity Catalog
-   Databricks Volumes
-   Medallion Architecture

## 3. Architecture

The project follows the Medallion Architecture:

**Landing → Bronze → Silver → Gold**

### Landing Layer

-   Stores the source CSV files.
-   Contains Orders, Payments, and Refunds files.
-   Contains a duplicate payments file for duplicate-upload testing.

### Bronze Layer

-   Reads source CSV files from the Landing Volume.
-   Stores raw data as Delta tables.
-   Preserves source data for downstream processing.

### Silver Layer

-   Validates mandatory columns and business rules.
-   Removes duplicate records using unique identifiers.
-   Cleans and structures Orders, Payments, and Refunds data.
-   Stores clean Delta tables.

### Gold Layer

-   Combines order, payment, and refund information.
-   Calculates order-level payment metrics.
-   Produces a daily payment summary for reporting.
-   Stores business-ready Delta tables and Volume copies.

## 4. Catalog, Schemas, and Volumes

**Catalog:** `online_shop_catalog`

**Schemas:** - `bronze` - `silver` - `gold`

**Volume locations:**

``` text
/Volumes/online_shop_catalog/default/landing_volume/
/Volumes/online_shop_catalog/bronze/bronze_volume/
/Volumes/online_shop_catalog/silver/silver_volume/
/Volumes/online_shop_catalog/gold/gold_volume/
```

### Landing folder structure

``` text
landing_volume/
├── source_files/
│   ├── orders.csv
│   ├── payments.csv
│   └── refunds.csv
└── duplicate_test/
    └── payments_duplicate_upload.csv
```

### Gold Volume folder structure

``` text
gold_volume/
├── order_payment_summary/
└── daily_payment_summary/
```

## 5. Notebook Execution Order

  -------------------------------------------------------------------------------------
  Notebook                                          Purpose
  ------------------------------------------------- -----------------------------------
  `01_orders_csv_ingest_from_landing_to_bronze`     Ingest Orders CSV into Bronze

  `02_payments_csv_ingest_from_landing_to_bronze`   Ingest Payments CSV into Bronze

  `03_refunds_csv_ingest_from_landing_to_bronze`    Ingest Refunds CSV into Bronze

  `04_bronze_orders_to_silver`                      Clean and load Orders into Silver

  `05_bronze_payments_to_silver`                    Clean and load Payments into Silver

  `06_bronze_refunds_to_silver`                     Clean and load Refunds into Silver

  `07_silver_to_gold_daily_payment_report`          Build the order-level Gold summary

  `08_gold_daily_payment_summary`                   Build the daily Gold summary
  -------------------------------------------------------------------------------------

## 6. Gold Tables

### 6.1 Gold Order Payment Summary

**Table:**

`online_shop_catalog.gold.gold_order_payment_summary`

**Gold Volume path:**

`/Volumes/online_shop_catalog/gold/gold_volume/order_payment_summary/`

This table contains one row per order and includes:

-   `order_id`
-   `customer_id`
-   `order_date`
-   `order_amount`
-   `order_status`
-   `total_payment_amount`
-   `total_payment` -- number of successful payment transactions
-   `total_refund_amount`
-   `total_refunds` -- number of successful refund transactions
-   `outstanding_balance`
-   `net_retained_amount`

### 6.2 Gold Daily Payment Summary

**Table:**

`online_shop_catalog.gold.gold_daily_payment_summary`

**Gold Volume path:**

`/Volumes/online_shop_catalog/gold/gold_volume/daily_payment_summary/`

This table contains daily aggregated metrics:

-   `order_date`
-   `total_orders`
-   `total_order_amount`
-   `total_payment_amount`
-   `total_payments`
-   `total_refund_amount`
-   `total_refunds`
-   `outstanding_balance`
-   `net_retained_amount`

## 7. Business Rules and Calculations

-   **Total Orders:** Count of distinct order IDs.
-   **Total Order Amount:** Sum of order amounts.
-   **Total Payments:** Sum of successful payment amounts.
-   **Payment Count:** Count of successful payment transactions.
-   **Total Refunds:** Sum of successful refund amounts.
-   **Refund Count:** Count of successful refund transactions.
-   **Outstanding Balance:** Order amount minus successful payments
    allocated to that order.
-   **Net Retained Amount:** Successful payments minus successful
    refunds.

Refunds must not reduce the customer's outstanding balance. Refund
reporting uses `refund_date`, not `received_date`, so a late-arriving
refund can update the report for the date on which it was issued.

## 8. Sample Output -- Gold Order Payment Summary

The following rows are from the displayed order-level output:

  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  order_id   customer_id   order_date     order_amount order_status     total_payment_amount   total_payment   total_refund_amount   total_refunds   outstanding_balance   net_retained_amount
  ---------- ------------- ------------ -------------- -------------- ---------------------- --------------- --------------------- --------------- --------------------- ---------------------
  O1002      C002          2026-11-26             1500 PAID                             1500               1                   200               1                  1500                   200

  O1003      C003          2026-11-26              750 PAID                              750               1                     0               0                   750                     0

  O1017      C017          2026-11-26             1100 PAID                             1100               1                     0               0                  1100                     0

  O1011      C011          2026-11-26              650 PAID                              650               1                     0               0                   650                     0

  O1018      C018          2026-11-26             1600 PARTIAL                           800               1                     0               0                   800                     0

  O1006      C006          2026-11-26             1200 PAID                             1200               1                     0               0                  1200                     0

  O1019      C019          2026-11-26              550 PAID                              550               1                     0               0                   550                     0

  O1001      C001          2026-11-26             1000 PAID                             1000               2                     0               0                  1000                     0

  O1009      C009          2026-11-26             1000 PAID                             1000               1                     0               0                  1000                     0

  O1010      C010          2026-11-26             2200 UNPAID                              0               0                     0               0                     0                     0

  O1016      C016          2026-11-26              800 UNPAID                              0               0                     0               0                     0                     0

  O1004      C004          2026-11-26             2000 PARTIAL                          1000               1                     0               0                  1000                     0

  O1005      C005          2026-11-26              500 PAID                              500               1                     0               0                   500                     0

  O1014      C014          2026-11-26              400 PAID                              400               1                     0               0                   400                     0

  O1015      C015          2026-11-26             2500 PAID                             2500               1                     0               0                  2500                     0

  O1007      C007          2026-11-26              300 PAID                              300               1                     0               0                   300                     0
  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

**Validation note:** The displayed values in `outstanding_balance` and
`net_retained_amount` above do not match the stated business formulas.
Recalculate these two columns before treating this order-level output as
final. For example: - O1001: outstanding = ₹0; net retained = ₹1,000. -
O1002: outstanding = ₹0; net retained = ₹1,300.

After recalculating, overwrite the Gold order-level table and reload its
Gold Volume copy.

## 9. Sample Output -- Gold Daily Payment Summary

The daily summary output for 2026-11-26 is:

  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  order_date     total_orders   total_order_amount   total_payment_amount   total_payments   total_refund_amount   total_refunds   outstanding_balance   net_retained_amount
  ------------ -------------- -------------------- ---------------------- ---------------- --------------------- --------------- --------------------- ---------------------
  2026-11-26               20                23450                  17150               19                   200               1                  6300                 16950

  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### Daily summary interpretation

-   Total orders: 20
-   Total order amount: ₹23,450
-   Successful payment amount: ₹17,150
-   Successful payment transactions: 19
-   Successful refund amount: ₹200
-   Successful refund transactions: 1
-   Outstanding balance: ₹6,300
-   Net retained amount: ₹16,950

These daily totals match the sample dataset's expected totals.

## 10. Test Scenarios

### Scenario 1: Multiple Payments for One Order

Order `O1001` has two successful payments: - Payment 1: ₹500 - Payment
2: ₹500

Expected order-level result: - One order - Total payments: ₹1,000 -
Payment transaction count: 2 - Outstanding balance: ₹0 - Net retained
amount: ₹1,000

### Scenario 2: Duplicate Payments File Upload

Upload the same payments file again. Deduplicate using `payment_id` so
the same transaction is not counted twice.

Expected result: Payment totals remain unchanged.

### Scenario 3: Late-Arriving Refund

Order `O1002` has: - Successful payment: ₹1,500 - Refund amount: ₹200 -
Refund date: 2026-11-26 - Received date: 2026-11-27

Expected order-level result: - Total payments: ₹1,500 - Total refunds:
₹200 - Outstanding balance: ₹0 - Net retained amount: ₹1,300

Use `refund_date` for the business report, not `received_date`.

## 11. Project Outcome

This project demonstrates: - CSV ingestion into a Bronze layer. - Data
validation and deduplication in a Silver layer. - PySpark
transformations and SQL-based Delta table creation. - Aggregation of
multiple payment transactions per order. - Calculation of outstanding
balances and net retained amounts. - Daily payment reporting. - Storage
of Gold Delta data in Databricks Volumes. - Testing of duplicate uploads
and late-arriving refunds.

**Project:** Online Shop Daily Payment Report\
**Platform:** Databricks\
**Processing:** PySpark and SQL\
**Architecture:** Medallion Architecture\
**Storage:** Delta Lake
