# Standard libraries
import os
import numpy as np
import pandas as pd
from math import ceil
import matplotlib.pyplot as plt
from functools import reduce
from joblib import Parallel, delayed
from IPython.display import display, HTML

from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql import functions as F

from pyspark.sql.functions import (
    coalesce, corr, array, countDistinct, approx_count_distinct,
    col, lit, expr, when, count, count_if, row_number, sum as spark_sum, abs as spark_abs,
    round as spark_round, min as spark_min, max as spark_max, avg as spark_avg,
    first, last, lag, lead, row_number, desc, asc, bool_or, floor,
    explode, sequence, from_unixtime, to_date, unix_timestamp,
    window, min_by, mode, concat, monotonically_increasing_id, mean, rand
)


#--------------------------SPARK SESSION ----------------------------------

def CreateSparkSession(Maxcores, partition, Nexecutors, MEMexec, log = False):
    os.environ["PYSPARK_PYTHON"] = "/opt/miniconda3/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/miniconda3/bin/python"
    
    configs = {
       "spark.scheduler.mode": "FAIR",                                  # Multi-user scheduler
       "spark.scheduler.pool": 'user_c',                                    # User pool
       "spark.scheduler.allocation.file": "file:///usr/local/spark/conf/fairscheduler.xml",  # Scheduler config file
       "spark.executor.memory": MEMexec,                                # Executor RAM
       "spark.executor.instances": Nexecutors,                          # Number of executors
       "spark.cores.max": Maxcores,                                         # Total cores
       "spark.sql.shuffle.partitions": partition,                      # Shuffle partitions
       "spark.sql.execution.arrow.pyspark.enabled": "true",             # Enable Arrow
       "spark.sql.execution.arrow.pyspark.fallback.enabled": "false",   # No Arrow fallback
       "spark.dynamicAllocation.enabled": "false",                      # No dynamic allocation
       "spark.shuffle.service.enabled": "false",                        # No shuffle service
       "spark.ui.port": 4042,                                        # Custom UI port
       "spark.sql.debug.maxToStringFields": 1000,                        # Debug fields limit
        "spark.memory.fraction": 0.6,                                    # fraction of heap for execution + storage
        "spark.memory.storageFraction": 0.6,                           # fraction of heap reserved for storage (persist/cache)
        "spark.driver.memory": "1g", 
        "spark.executor.memoryOverhead": "1g"
    }
    
    spark_builder = SparkSession.builder \
       .appName("ProjectCloudVeneto") \
       .master("spark://10.67.22.135:7077")
    
    for key, value in configs.items():
       spark_builder = spark_builder.config(key, value)
        
    
    spark = spark_builder.getOrCreate()
    if log:
        spark.sparkContext.setLogLevel("ERROR")

    return spark

#--------------------------PIVOTING THE DATASET ----------------------------------
def Pivot(df):

    df_all_hw = (df.groupBy("hwid", "when")
                   .pivot("metric")
                   .agg(first("value"))
                   .withColumn("time", from_unixtime(col("when")))
                   .orderBy("hwid", "when"))
    return df_all_hw


#--------------------------PREPROCESSING PIPELINE ----------------------------------
#Create a grid of "interval" seconds in order to have homogeneous separation of data. 
def CreateGrid(df, interval):

    #time window interval column
    df_windowed = df.withColumn("time_window", window("time", f"{interval} seconds"))

    aggs = []
    sensors = [c for c in df.columns if c not in ("when", "time", "hwid")]
    for s in sensors:
        #handle different datatypes: mean continuous metric and take min/max of binary ones
        stats = df.selectExpr(f"min({s}) as min", f"max({s}) as max").first()
        is_binary = stats["min"] is not None and stats["max"] is not None    and    0 <= stats["min"] and stats["max"] <= 1
        
        # treat differently A5 and A9 sensors 
        if s in ["A5", "A9"] or is_binary:
            agg_func = spark_max(col(s)).alias(s)
        else:
            agg_func = spark_avg(col(s)).alias(s)
        aggs.append(agg_func)

    #groupy by hardware and time_window (homogeneous resampling)
    result_df = (
        df_windowed
        .groupBy("time_window")
        .agg(*aggs)
    )

    result_df = (
        result_df
        .withColumn("window_start", col("time_window.start"))
        .withColumn("window_end", col("time_window.end"))
        .withColumn("when", expr("unix_timestamp(window_start) + int((unix_timestamp(window_end) - unix_timestamp(window_start)) / 2)"))
        .drop("time_window")
        .orderBy("when")
    )

    return result_df.select(["when","window_start","window_end"]+sensors)


#Given the dataset, creates another column with the block id given the max_interval between two data points
#Create independent blocks in the dataset when the time difference between two record is grater than max_interval
def BuildBlocks(df, max_interval, sensors):    

    #Parallelized on the 4 hardware
    w_hw = Window.partitionBy(F.lit(1)).orderBy("when")
    #Compute previous and next timestamp
    df = (df
          .withColumn("Prev_TimeStamp", lag("when").over(w_hw))
          .withColumn("Next_TimeStamp", lead("when").over(w_hw))
          .withColumn("PrevDiff", col("when") - col("Prev_TimeStamp"))
          .withColumn("NextDiff", col("Next_TimeStamp") - col("when"))
         )

    #Check if the timediff between two consecutive data is more than max_interval
    df = df.withColumn("CheckNewBlock", when(col("PrevDiff") > max_interval, 1).otherwise(0))
    df = df.withColumn("BlockID", spark_sum("CheckNewBlock").over(w_hw))

    df = df.drop("CheckNewBlock")

    return df


#Fill most of all values inside the arbitraty time gap
def FillNull(df, sensors, max_gap=240):
    w = Window.partitionBy("BlockID").orderBy("when")
    
    for s in sensors:
        prev_val = lag(col(s)).over(w)
        next_val = lead(col(s)).over(w)
        
        df = df.withColumn(s, when(col(s).isNotNull(), col(s))
            .when(
                (col("NextDiff") <= max_gap) & 
                (col("NextDiff") <= col("PrevDiff")), 
                next_val).when(col("PrevDiff") <= max_gap, prev_val))

    
    df = df.na.drop(subset=sensors)
    
    return df




def UsefulSensors(df_blocks, sensors):
    df_max = df_blocks.select(*sensors).groupBy().agg( *[spark_max(s).alias(s) for s in sensors] )
    max_values = df_max.first().asDict()
    
    df_min = df_blocks.select(*sensors).groupBy().agg( *[spark_min(s).alias(s) for s in sensors] )
    min_values = df_min.first().asDict()
    
    useless_sensors = [k for k in sensors if max_values[k] == min_values[k]]
    useful_sensors = [k for k in sensors if k not in useless_sensors]


    return useless_sensors, useful_sensors





#----------------------------ANOMALY DETECTION
def detect_anomalies(df, time_separator, threshold, sensors, partition):
    '''
    Crea colonne con flag per anomalie.
    '''
    # Lag to get previous value within each partition (i.e. within each block)
    df = df.repartition(partition, "BlockID")
    window         = Window.partitionBy("BlockID").orderBy("when")
    lagged_columns = [lag(col(s)).over(window) for s in sensors] 
    lag_names      = [f"lagged_{s}" for s in sensors]
    
    df_lagged = df.withColumns(dict(zip(lag_names, lagged_columns)))

    # Determina switch del sensore (didSwitch = 1 se il sensore passa da 0 a 1 o viceversa, didSwitch = 0 altrimenti)
    didSwitch    = [when((col(f"lagged_{s}") != col(s)), 1).otherwise(0) for s in sensors] 
    switch_names = [f"didSwitch_{s}" for s in sensors]    

    df_didSwitch = df_lagged.withColumns(dict(zip(switch_names, didSwitch)))

    # Detect anomaly group: when two clusters are more distant than time_separator they are grouped as different anomalies
    # Tutte le anomalie di uno stesso gruppo hanno stesso id, cioè un numero crescente che si resetta ad ogni nuovo blocco
    df_anomalies = df_didSwitch
    for sensor in sensors:   
        # Il periodo anomalo inizia quando il sensore è 1 e finisce quando esso è 0
        df_start = (
            df_didSwitch \
            .withColumn(f'theres0Before_{sensor}', count_if(col(sensor) == 0).over(window.rangeBetween(1, time_separator)) > 0) \
            .withColumn(f'theres0After_{sensor}', count_if(col(sensor) == 0).over(window.rangeBetween(-time_separator, -1)) > 0) \
            .filter( 
                (col(f'didSwitch_{sensor}') == 1) & 
                (when(col(sensor) == 1, col(f'theres0Before_{sensor}')).otherwise(True)) &
                (when(col(sensor) == 0, col(f'theres0After_{sensor}')).otherwise(True))
            ) \
            .withColumn(f'startGroup_{sensor}', when((col('when')-lag(col('when'), 1, -1e9).over(window))>time_separator, 1).otherwise(0)) \
            .withColumn(f'anomalyID_{sensor}', spark_sum(col(f'startGroup_{sensor}')).over(window)) \
        )
        
        df_anomalies = df_anomalies.join(
            other = df_start.select('BlockID', 'when', f'startGroup_{sensor}', f'anomalyID_{sensor}'),
            on = ['BlockID', 'when'],
            how = 'left'
        )

    count_names = [f'count_{s}' for s in sensors]
    count_cols  = [count('*').over(Window.partitionBy('BlockID', f'anomalyID_{s}')) for s in sensors]

    flag_names  = [f'flag_{s}' for s in sensors]
    flag_cols   = [when((col(f'count_{s}') >= threshold) & (col(f'anomalyID_{s}') > 0), True).otherwise(False) for s in sensors]
    
    df_flag = df_anomalies \
        .withColumns(dict(zip(count_names, count_cols))) \
        .withColumns(dict(zip(flag_names, flag_cols))) \
        .orderBy('BlockID', 'when')

    # Quando ci sono delle righe comprese tra anomalie con stesso ID, queste righe sono a loro volta considerate anomalie
    next_w  = Window.partitionBy('BlockID').orderBy('when').rangeBetween(0, time_separator)
    prev_w  = Window.partitionBy('BlockID').orderBy('when').rangeBetween(-time_separator, 0)

    prev_names = [f'prevID_{s}' for s in sensors]
    prev_id = [when((bool_or(f'flag_{s}').over(prev_w)), spark_max(f'anomalyID_{s}').over(prev_w)).otherwise(None) for s in sensors]
    next_names = [f'nextID_{s}' for s in sensors]
    next_id = [when((bool_or(f'flag_{s}').over(next_w)), spark_min(f'anomalyID_{s}').over(next_w)).otherwise(None) for s in sensors]

    df_newID = (
        df_flag \
            .withColumns(dict(zip(prev_names, prev_id))) \
            .withColumns(dict(zip(next_names, next_id)))
    )

    new_flags = [when((col(f'nextID_{s}') == col(f'prevID_{s}')) & (col(f'nextID_{s}') > 0), True).otherwise(col(f'flag_{s}')) for s in sensors]
    
    df_flag = df_newID.withColumns(dict(zip(flag_names, new_flags)))
    
    condition = reduce(lambda a, b: a | b, [col(f'flag_{s}') for s in sensors])
    df_flag = df_flag.withColumn('flag_anomaly', when(condition, 1).otherwise(0))

    return df_flag \
        .select("BlockID", "when", *sensors, *flag_names, 'flag_anomaly') 




#------------------------CORRELATIONS----------------------------

def correlations(df, sensors_list, target_col, batch_size=25):
        
    # Create correlation expressions for current batch
    corr_expressions = [corr(target_col, sensor).alias(f"corr_{sensor}") for sensor in sensors_list]
    
    # Execute correlations for this batch
    batch_results = df.agg(*corr_expressions).collect()[0]

    batch_correlations = [batch_results[f"corr_{sensor}"] or 0.0 for sensor in sensors_list]
    
    # Create DataFrame with results
    results_df = pd.DataFrame({
        "Sensors": sensors_list, 
        "Correlations": batch_correlations
    })
    
    # Sort by absolute correlation (highest first)
    sorted_results = results_df.reindex(
        results_df["Correlations"].abs().sort_values(ascending=False).index
    ).reset_index(drop=True)
    
    return sorted_results


#------------------------PREDICTIVE MAINTEINANCE--------------------------

def extract_alarms(df, columns=["A5", "A9"], bits=[6, 7, 8]):
    for col_name in columns:
        for bit in bits:
            convert_bit = bit - 1  # bit 1 is LSB
            df = df.withColumn( f"{bit}-{col_name}", ((col(col_name).bitwiseAND(1 << convert_bit)) > 0).cast("int") )

    df = df.withColumn(
        "overheating",
        when(
            (col("6-A5") == 1) | (col("7-A5") == 1) | (col("8-A5") == 1) | (col("6-A9") == 1) | (col("7-A9") == 1) | (col("8-A9") == 1), 1)
            .otherwise(0).cast("int")).where( (col("A5").isNotNull()) | (col("A9").isNotNull()) )
    return df


def add_predictive(df, target, window_before_heating=30, join=True ,debug=False, partition = 10):

    df = df.repartition(partition, "BlockID")
    w = Window.partitionBy("BlockID").orderBy("when")

    df_pred = df.select("BlockID","when","window_start",target)
    df_pred = df.withColumn(f"prev_{target}", lag(target).over(w))
    df_pred = df_pred.withColumn(
        f"is_start_{target}",
        when(
            (col(target) == 1) &
            ((col(f"prev_{target}") != 1) | col(f"prev_{target}").isNull()),
            1
        ).otherwise(0)
    )

    df_pred = df_pred.withColumn(f"start_time_{target}", when(col(f"is_start_{target}") == 1, col("when")))

    w_future = w.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    df_pred = df_pred.withColumn(
        f"next_start_{target}",
        first(f"start_time_{target}", ignorenulls=True).over(w_future)
    )

    window_seconds = window_before_heating * 60
    df_pred = df_pred.withColumn(
    f"predictive_{target}",
        when(
            (col(f"next_start_{target}").isNotNull()) &
            (((col(target).isNull()) | (col(target) == 0))) &
            ((col(f"next_start_{target}") - col("when")) > 0) &
            ((col(f"next_start_{target}") - col("when")) <= window_seconds), 1 ).otherwise(0))

    if not debug:
        df_pred = df_pred.select("when",target,f"predictive_{target}")


    if join:
        return df.join( df_pred.select('when', f'predictive_{target}') , on='when', how='left' )
    else:
        return df_pred