# Databricks notebook source
from pyspark.sql.functions import *

df_bronze1 = spark.read.option("header", True).option("inferschema", True).csv("dbfs:/FileStore/Crimes___2022_20240929.csv")


print(df_bronze1.columns)

new_column_names = [col.replace(" ", "_").replace(",", "").replace(";", "").replace("(", "").replace(")", "").replace("{", "").replace("}", "").replace("\n", "").replace("\t", "").replace("=", "") for col in df_bronze1.columns]


for old_name, new_name in zip(df_bronze1.columns, new_column_names):
    df_bronze1 = df_bronze1.withColumnRenamed(old_name, new_name)

df_bronze1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze1_table")


# COMMAND ----------

display(df_bronze1)

# COMMAND ----------


valid_column_names = [c.replace(" ", "_").replace(",", "").replace(";", "") \
                      .replace("{", "").replace("}", "").replace("(", "") \
                      .replace(")", "").replace("\n", "").replace("\t", "") \
                      .replace("=", "") for c in df_bronze1.columns]


df_silver1_valid_columns = df_bronze1.toDF(*valid_column_names)


df_silver1_filtered = df_silver1_valid_columns.filter(
    df_silver1_valid_columns["X_Coordinate"].isNotNull() & df_silver1_valid_columns["Y_Coordinate"].isNotNull()
)

df_silver1_filtered.write.format("delta") \
                          .mode("overwrite") \
                          .option("overwriteSchema", "true") \
                          .saveAsTable("silver1_table")

display(df_silver1_filtered)

# COMMAND ----------

from pyspark.sql.functions import count


df_gold1 =df_silver1_filtered.groupBy("Year").agg(count("arrest").alias("arrest_count"))

df_gold1.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("createTableColumnTypes", "Year STRING, arrest_count LONG") \
    .saveAsTable("gold1_table")

display(df_gold1)

# COMMAND ----------

from pyspark.sql.functions import count, col, when


df_gold1 = df_silver1_filtered.groupBy("Year") \
    .agg(count(when(col("arrest") == "true", True)).alias("arrest_true"),
         count(when(col("arrest") == "false", True)).alias("arrest_false"))

df_gold1.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold1_table")

display(df_gold1)

# COMMAND ----------

from pyspark.sql.functions import count, col, when


df_gold1 = df_silver1_filtered.groupBy("Year") \
    .agg(
        count(when(col("arrest") == True, True)).alias("arrest_true"),  
        count(when(col("arrest") == False, True)).alias("arrest_false"),
        count(when(col("Location_Description") == "STREET", True)).alias("Location_Description")
    )


df_gold1.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold1_table")


display(df_gold1)


# COMMAND ----------

crime_district_df = (
    df_silver1_filtered
    .groupBy(col("District").alias("Crime_District"))
    .agg(
        count("*").alias("Total_Crimes"),  # Count of each Crime_Type
        sum(when(col("Domestic"), 1).otherwise(0)).alias("Domestic")
    )
)

display(crime_district_df)
