from pyspark.sql.functions import (
    col, current_timestamp, to_date, year, month, coalesce, lit,
    avg, sum as spark_sum, max as spark_max, min as spark_min, count, when
)

df = spark.table("bronze_noaa_weather")

# Clean and pivot daily data
df_clean = df \
    .withColumn("date_clean", to_date(col("date").substr(1, 10), "yyyy-MM-dd")) \
    .select("date_clean", "state", "datatype", "value", "ingested_at")

df_ingested = df_clean.groupBy("date_clean", "state").agg(
    spark_max("ingested_at").alias("ingested_at")
)

df_pivoted = df_clean.groupBy("date_clean", "state") \
    .pivot("datatype", ["TMAX", "TMIN", "PRCP", "AWND", "SNOW", "TAVG"]) \
    .max("value")

df_pivoted = df_pivoted.join(df_ingested, on=["date_clean", "state"], how="left")

df_daily = df_pivoted \
    .withColumn("PRCP", coalesce(col("PRCP"), lit(0.0))) \
    .withColumn("SNOW", coalesce(col("SNOW"), lit(0.0))) \
    .withColumn("AWND", coalesce(col("AWND"), lit(0.0))) \
    .withColumn("year", year(col("date_clean"))) \
    .withColumn("month", month(col("date_clean"))) \
    .withColumnRenamed("date_clean", "date") \
    .withColumnRenamed("TMAX", "temperature_max_f") \
    .withColumnRenamed("TMIN", "temperature_min_f") \
    .withColumnRenamed("TAVG", "temperature_avg_f_reported") \
    .withColumnRenamed("PRCP", "precipitation_inches") \
    .withColumnRenamed("AWND", "wind_speed_avg_mph") \
    .withColumnRenamed("SNOW", "snowfall_inches") \
    .withColumn("temperature_max_f", col("temperature_max_f").cast("decimal(10,2)")) \
    .withColumn("temperature_min_f", col("temperature_min_f").cast("decimal(10,2)")) \
    .withColumn("temperature_avg_f_reported", col("temperature_avg_f_reported").cast("decimal(10,2)")) \
    .withColumn("precipitation_inches", col("precipitation_inches").cast("decimal(10,2)")) \
    .withColumn("wind_speed_avg_mph", col("wind_speed_avg_mph").cast("decimal(10,2)")) \
    .withColumn("snowfall_inches", col("snowfall_inches").cast("decimal(10,2)")) \
    .withColumn(
        "temperature_avg_f",
        coalesce(
            col("temperature_avg_f_reported"),
            ((col("temperature_max_f") + col("temperature_min_f")) / 2).cast("decimal(10,2)")
        )
    ) \
    .withColumn("transformed_at", current_timestamp())

print("=== Daily Silver ===")
df_daily.printSchema()
print(f"Row count: {df_daily.count()}")
display(df_daily.limit(10))

# Aggregate to monthly summary
df_monthly = df_daily.groupBy("state", "year", "month").agg(
    avg("temperature_max_f").alias("avg_temp_max_f"),
    avg("temperature_min_f").alias("avg_temp_min_f"),
    avg("temperature_avg_f").alias("avg_temp_f"),
    spark_max("temperature_max_f").alias("highest_temp_f"),
    spark_min("temperature_min_f").alias("lowest_temp_f"),
    spark_sum("precipitation_inches").alias("total_precipitation_inches"),
    spark_sum("snowfall_inches").alias("total_snowfall_inches"),
    avg("wind_speed_avg_mph").alias("avg_wind_speed_mph"),
    count(when(col("precipitation_inches") > 0, 1)).alias("days_with_precipitation"),
    count(when(col("snowfall_inches") > 0, 1)).alias("days_with_snow"),
    spark_max("ingested_at").alias("ingested_at")
) \
.withColumn("avg_temp_max_f", col("avg_temp_max_f").cast("decimal(10,2)")) \
.withColumn("avg_temp_min_f", col("avg_temp_min_f").cast("decimal(10,2)")) \
.withColumn("avg_temp_f", col("avg_temp_f").cast("decimal(10,2)")) \
.withColumn("highest_temp_f", col("highest_temp_f").cast("decimal(10,2)")) \
.withColumn("lowest_temp_f", col("lowest_temp_f").cast("decimal(10,2)")) \
.withColumn("total_precipitation_inches", col("total_precipitation_inches").cast("decimal(10,2)")) \
.withColumn("total_snowfall_inches", col("total_snowfall_inches").cast("decimal(10,2)")) \
.withColumn("avg_wind_speed_mph", col("avg_wind_speed_mph").cast("decimal(10,2)")) \
.withColumn("transformed_at", current_timestamp())

print("\n=== Monthly Silver ===")
df_monthly.printSchema()
print(f"Row count: {df_monthly.count()}")
display(df_monthly.limit(10))

df_daily.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_noaa_weather_daily")

print(f"Written {df_daily.count()} records to silver_noaa_weather_daily")

df_monthly.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_noaa_weather_monthly")

print(f"Written {df_monthly.count()} records to silver_noaa_weather_monthly")