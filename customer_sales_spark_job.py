from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap
from pyspark.sql.functions import to_date
from pyspark.sql.functions import year, month

spark = SparkSession.builder.appName("CustomerSalesETL").getOrCreate()

# Read the csv file from the GCP bucket
df = spark.read.csv('gs://customer-sales-data-bucket1/sales_data/customer_sales_pipeline_dataset.csv',  header=True, inferSchema=True)

# Rename the Columns
df = df.withColumnRenamed("OrderID", "Order_ID") \
                   .withColumnRenamed("CustomerName", "Customer_Name") \
                   .withColumnRenamed("ProductCategory", "Product_Category")\
                   .withColumnRenamed("UnitPrice", "Unit_Price")\
                   .withColumnRenamed("TotalPrice", "Total_Price")\
                   .withColumnRenamed("OrderDate", "Order_Date")\
                   .withColumnRenamed("PaymentMethod", "Payment_Method")

# Load and transform
df_clean = df.drop("Email", "Phone")

# Filter out invalid rows
df_clean = df_clean.filter(
    (col("Order_ID").isNotNull()) & 
    (col("Quantity") > 0) & 
    (col("Unit_Price").isNotNull()) & 
    (col("Total_Price").isNotNull())
)

# Remove extra spaces in text columns
text_columns = ["Customer_Name", "Product", "Product_Category", "City", "State", "Payment_Method"]
for column in text_columns:
    df_clean = df_clean.withColumn(column, trim(col(column)))

# Convert order_date to proper date format
df_clean = df_clean.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))

# Extract year and month
df_clean = df_clean.withColumn("Year", year(col("Order_Date"))) \
                   .withColumn("Month", month(col("Order_Date")))

# Verify whether Total_Price equals Quantity * Unit_Price
df_check = df_clean.withColumn("Computed_Total", col("Quantity") * col("Unit_Price")) \
                   .withColumn("is_Correct", col("Computed_Total") == col("Total_Price"))

# Write to BigQuery
df_clean.write \
    .format("bigquery") \
    .option("table", "david-etl-project.customer_sales.cleaned_sales_data") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()