from pyspark.sql.functions import col, current_timestamp, year, month, to_date

# Read from Bronze
df = spark.table("bronze_eia_electricity_prices")

# Clean and transform
df_silver = df \
    .withColumnRenamed("price-units", "price_units") \
    .withColumn("price", col("price").cast("decimal(10,2)")) \
    .filter(col("price").isNotNull()) \
    .withColumn("period_date", to_date(col("period"), "yyyy-MM")) \
    .withColumn("year", year(col("period_date"))) \
    .withColumn("month", month(col("period_date"))) \
    .withColumn("transformed_at", current_timestamp()) \
    .withColumnRenamed("stateid", "state")

# Check schema and preview before writing
df_silver.printSchema()
display(df_silver.limit(10))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_eia_electricity_prices")

print(f"Written {df_silver.count()} records to silver_eia_electricity_prices")