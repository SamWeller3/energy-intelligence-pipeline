from pyspark.sql.functions import col, current_timestamp

df_prices = spark.table("silver_eia_electricity_prices")
df_generation = spark.table("silver_eia_generation")
df_weather = spark.table("silver_noaa_weather_monthly")

# Overall price (ALL sectors)
df_prices_all = df_prices.filter(col("sectorid") == "ALL") \
    .select("state", "year", "month", "price", "price_units")

# Sector breakdown, pivoted into columns
df_prices_sector = df_prices.filter(col("sectorid") != "ALL") \
    .groupBy("state", "year", "month") \
    .pivot("sectorid", ["RES", "COM", "IND", "TRA", "OTH"]) \
    .sum("price") \
    .withColumnRenamed("RES", "price_residential") \
    .withColumnRenamed("COM", "price_commercial") \
    .withColumnRenamed("IND", "price_industrial") \
    .withColumnRenamed("TRA", "price_transportation") \
    .withColumnRenamed("OTH", "price_other")

# Generation, filtered and pivoted
df_generation_relevant = df_generation.filter(col("sectorid") == "99") \
    .filter(col("fueltypeid").isin(["NG", "COL", "NUC", "SUN", "WND", "HYC"])) \
    .groupBy("state", "year", "month") \
    .pivot("fueltypeid", ["NG", "COL", "NUC", "SUN", "WND", "HYC"]) \
    .sum("generation") \
    .withColumnRenamed("NG", "generation_natural_gas") \
    .withColumnRenamed("COL", "generation_coal") \
    .withColumnRenamed("NUC", "generation_nuclear") \
    .withColumnRenamed("SUN", "generation_solar") \
    .withColumnRenamed("WND", "generation_wind") \
    .withColumnRenamed("HYC", "generation_hydro")

# Join everything together
df_gold_summary = df_prices_all \
    .join(df_prices_sector, on=["state", "year", "month"], how="left") \
    .join(df_weather, on=["state", "year", "month"], how="left") \
    .join(df_generation_relevant, on=["state", "year", "month"], how="left") \
    .withColumn("transformed_at", current_timestamp())

print(f"Row count: {df_gold_summary.count()}")
df_gold_summary.printSchema()
display(df_gold_summary.limit(10))

df_gold_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_monthly_state_summary")

print(f"Written {df_gold_summary.count()} records to gold_monthly_state_summary")