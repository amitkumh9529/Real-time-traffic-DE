"""SparkSession builder supporting both distributed cluster and local test modes."""

import os
import sys
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str,
    master: str = None,
    enable_hive: bool = True,
    local_mode: bool = False
) -> SparkSession:
    """Builds and returns a Delta Lake-enabled SparkSession.
    
    Args:
        app_name: Application name reported in Spark UI.
        master: Spark master URL (defaults to env SPARK_MASTER or local).
        enable_hive: Whether to enable Hive Metastore catalog integration.
        local_mode: If True, configures optimized local standalone instance.
    """
    spark_master = master or os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    
    if local_mode or spark_master.startswith("local"):
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        try:
            from delta import configure_spark_with_delta_pip
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            spark = builder.getOrCreate()
    else:
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(spark_master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        if enable_hive:
            builder = builder.enableHiveSupport()
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
