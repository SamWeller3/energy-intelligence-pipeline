from pyspark.sql.functions import col, current_timestamp, year, month, to_date

df = spark.table("bronze_eia_generation")

df_silver = df \
    .withColumnRenamed("generation-units", "generation_units") \
    .withColumn("generation", col("generation").cast("decimal(15,3)")) \
    .filter(col("generation").isNotNull()) \
    .withColumn("period_date", to_date(col("period"), "yyyy-MM")) \
    .withColumn("year", year(col("period_date"))) \
    .withColumn("month", month(col("period_date"))) \
    .withColumn("transformed_at", current_timestamp()) \
    .withColumnRenamed("location", "state")

df_silver.printSchema()
display(df_silver.limit(10))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_eia_generation")

print(f"Written {df_silver.count()} records to silver_eia_generation")