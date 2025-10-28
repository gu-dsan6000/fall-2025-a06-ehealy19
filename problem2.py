from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, min as spark_min, max as spark_max,
    to_timestamp, substring_index, count, desc
)
import matplotlib.pyplot as plt
import pandas as pd
import os, shutil
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import expr
import sys
import glob

def create_spark_session(master_url="local[*]"):
    """Create a Spark session for Problem 2"""
    spark = (
        SparkSession.builder
        .appName("Problem2")
        .master(master_url)
        .getOrCreate()
    )
    return spark

def save_csv_file(df, output_path):
    """Saving to CSV files instead of Spark default of folders."""
    temp_dir = output_path + "_temp"
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")][0]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.move(os.path.join(temp_dir, part_file), output_path)
    shutil.rmtree(temp_dir)

def solve_prob2(input_dir, master_url):
    """Computation section for counts, sample, and summary statistics."""

    # starting the spark session
    spark = create_spark_session(master_url)

    # defining the input and output paths
    output_timeseries = "data/output/problem2_timeline.csv"
    output_cluster = "data/output/problem2_cluster_summary.csv"
    output_summary = "data/output/problem2_stats.txt"
    output_barchart = "data/output/problem2_bar_chart.png"
    output_density = "data/output/problem2_density_plot.png"

    # reading in the log files
    spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    log_files = (
        glob.glob("/home/ubuntu/spark-cluster/raw/application_*/**/*.log", recursive=True)
        + glob.glob("/home/ubuntu/spark-cluster/raw/application_*/*.log")
    )
    print("DEBUG: Number of .log files matched:", len(log_files))
    print("First few:", log_files[:5])

    logs = spark.read.text(log_files)
    logs = logs.withColumn("file_path", input_file_name())
    logs = logs.withColumn("application_id", regexp_extract(col("file_path"), r"application_\d+_\d+", 0))
    logs = logs.withColumn("cluster_id", regexp_extract(col("application_id"), r"application_(\d+)_", 1))
    
    print("DEBUG: Number of rows loaded:", logs.count())
    logs.select("file_path").show(10, truncate=False)

    # getting the timeseries data
    time_pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
    logs = logs.withColumn("timestamp", regexp_extract(col("value"), time_pattern, 1))
    logs = logs.withColumn("timestamp", expr("try_to_timestamp(timestamp, 'yy/MM/dd HH:mm:ss')"))
    app_times = (
        logs.filter(col("timestamp").isNotNull())
        .groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("cluster_id", "start_time")
    )
    app_times = app_times.withColumn("app_number", substring_index(col("application_id"), "_", -1))
    app_times.toPandas().to_csv("/home/ubuntu/spark-cluster/data/output/problem2_timeline.csv", index=False)

    # getting the aggregated cluster statistics
    cluster_summary = (
        app_times.groupBy("cluster_id")
        .agg(
            count("*").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        )
        .orderBy(desc("num_applications"))
    )
    cluster_summary.toPandas().to_csv("/home/ubuntu/spark-cluster/data/output/problem2_cluster_summary.csv", index=False)

    # computing the summary statistics
    total_clusters = cluster_summary.count()
    total_apps = app_times.count()
    avg_apps_cluster = total_apps / total_clusters if total_clusters else 0
    top_clusters = cluster_summary.orderBy(desc("num_applications")).limit(5).collect()
    summary_lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps_cluster:.2f}",
        "",
        "Most heavily used clusters:"
    ]
    for row in top_clusters:
        summary_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    summary_text = "\n".join(summary_lines)
    os.makedirs(os.path.dirname(output_summary), exist_ok=True)
    with open(output_summary, "w") as f:
        f.write(summary_text)

    # creating the bar chart visualization
    cluster_pd = cluster_summary.toPandas()
    plt.figure(figsize=(8, 5))
    plt.bar(cluster_pd["cluster_id"], cluster_pd["num_applications"], color="skyblue")
    for i, v in enumerate(cluster_pd["num_applications"]):
        plt.text(i, v + 0.5, str(v), ha="center", va="bottom")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.title("Applications per Cluster")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(output_barchart)
    plt.close()

    # creating the faceted density plot visualization
    apps_pd = app_times.toPandas()
    apps_pd["duration_sec"] = (apps_pd["end_time"] - apps_pd["start_time"]).dt.total_seconds()
    plt.figure(figsize=(8, 5))
    plt.hist(apps_pd["duration_sec"], bins=30, alpha=0.6, color="steelblue", density=True)
    if len(apps_pd["duration_sec"].dropna()) > 1:
        apps_pd["duration_sec"].plot(kind="kde", color="red")
    plt.xscale("log")
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Density")
    plt.title(f"Job Duration Distribution Across All Clusters (n={len(apps_pd)})")
    plt.tight_layout()
    plt.savefig(output_density)
    plt.close()
    
    spark.stop()


def main():
    """Main function for problem 2."""
    master_url = sys.argv[1] if len(sys.argv) > 1 else "local[*]"
    input_path = "/home/ubuntu/spark-cluster/raw/application_*"
    solve_prob2(input_path, master_url)

if __name__ == "__main__":
    main()