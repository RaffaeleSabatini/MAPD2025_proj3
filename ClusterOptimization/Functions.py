# Standard libraries
import os
import numpy as np
import pandas as pd
from math import ceil
import matplotlib.pyplot as plt

# PySpark core
from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import IntegerType

# PySpark functions
from pyspark.sql.functions import (
    coalesce,
    col, lit, expr, when, count, sum as spark_sum, abs as spark_abs,
    round as spark_round, min as spark_min, max as spark_max, avg as spark_avg,
    first, last, lag, row_number, desc, asc,
    explode, sequence, from_unixtime, to_date, unix_timestamp,
    window, min_by, mode, concat, monotonically_increasing_id
)


#--------------------------SPARK SESSION ----------------------------------

def CreateSparkSession(core):
    os.environ["PYSPARK_PYTHON"] = "/opt/miniconda3/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/miniconda3/bin/python"
    
    spark = SparkSession.builder \
        .appName("ProjectCloudVeneto") \
        .master("spark://10.67.22.135:7077") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.pool", 'user_a') \
        .config("spark.scheduler.allocation.file", "file:///usr/local/spark/conf/fairscheduler.xml") \
        .config("spark.cores.max", core) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.ui.port", 4042) \
        .getOrCreate()
        #To suppress the initial warning
        #.spark.sparkContext.setLogLevel("ERROR")
        

    sc = spark.sparkContext

    return spark



#--------------------------PREPROCESSING THE DATAFRAME ----------------------------------
def FillGaps(df, sensors = None, interval = 60, modality = 'auto'):

    # 1. Infer sensor columns if not provided
    if sensors is None:
        sensors = [c for c in df.columns if c not in ("when", "time")]

    # 2. Add timestamp column
    df_ts = df.withColumn("timestamp", from_unixtime(col("when")).cast("timestamp"))

    # 3. Create time window
    df_windowed = df_ts.withColumn("time_window", window("timestamp", f"{interval} seconds"))

    # 4. Aggregate using selected modality
    if modality == "mode":
        # Special case: MODE needs groupBy and count per window + sensor
        aggs = []
        for s in sensors:
            mode_df = (
                df_windowed.groupBy("time_window", s)
                .agg(count("*").alias("cnt"))
                .withColumn("rank", row_number().over(
                    Window.partitionBy("time_window").orderBy(desc("cnt"))
                ))
                .filter(col("rank") == 1)
                .select("time_window", col(s).alias(s))
            )
            if not aggs:
                result_df = mode_df
            else:
                result_df = result_df.join(mode_df, on="time_window", how="outer")
    else:
        aggs = []
        for s in sensors:
            if modality == "mean":
                agg_func = spark_avg(col(s)).alias(s)
            elif modality == "min":
                agg_func = spark_min(col(s)).alias(s)
            elif modality == "max":
                agg_func = spark_max(col(s)).alias(s)
            elif modality == "auto":
                stats = df.selectExpr(f"min({s}) as min", f"max({s}) as max").first()
                is_binary = stats["min"] is not None and stats["max"] is not None and 0 <= stats["min"] and stats["max"] <= 1
                if s in ["A5", "A9"] or is_binary:
                    agg_func = spark_max(col(s)).alias(s)
                else:
                    agg_func = spark_avg(col(s)).alias(s)
            else:
                raise ValueError(f"Unsupported modality: {modality}")
            aggs.append(agg_func)

        result_df = (
            df_windowed
            .groupBy("time_window")
            .agg(*aggs)
        )

    # 5. Add window_start, window_end, and 'when' as center of window
    result_df = (
        result_df
        .withColumn("window_start", col("time_window.start"))
        .withColumn("window_end", col("time_window.end"))
        .withColumn("when", expr("unix_timestamp(window_start) + int((unix_timestamp(window_end) - unix_timestamp(window_start)) / 2)"))
        .drop("time_window")
        .orderBy("when")
    )

    # 6. Add window_id as progressive row number
    result_df = result_df.withColumn("window_id", monotonically_increasing_id())

    return result_df.select(["window_id", "when", "window_start", "window_end"] + sensors)


#Given the dataset, creates another column with the block id given the max_interval between two data points
def BuildBlocks(df, max_interval):

    #Computes a new column with the time difference between next timestamp
    w = Window.partitionBy(lit(1)).orderBy("when")

    df = df.withColumn("Prev_TimeStamp", lag("when").over(w))
    df = df.withColumn("TimeDiff_s", col("when") - col("Prev_TimeStamp"))

    #Handle the first NULL value 
    df = df.withColumn("TimeDiff_s", coalesce(col("TimeDiff_s"), lit(60)))

    #Define blocks_id
    df = df.withColumn("CheckNewBlock", when(col("TimeDiff_s") > max_interval, 1).otherwise(0))
    df = df.withColumn("BlockID", spark_sum("CheckNewBlock").over(w))

    return df
    

