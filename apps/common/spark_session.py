import os

from pyspark.sql import SparkSession


def _ensure_windows_hadoop_env():
    """Configures HADOOP_HOME and SPARK_HOME environment."""
    import pyspark
    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
        if os.path.exists(hadoop_home):
            os.environ.setdefault("HADOOP_HOME", hadoop_home)
            hadoop_bin = os.path.join(hadoop_home, "bin")
            if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{hadoop_bin};" + os.environ.get("PATH", "")


def get_spark_session(
    app_name: str,
    master: str | None = None,
    enable_hive: bool = True,
    local_mode: bool = False,
) -> SparkSession:
    """Builds and returns a Delta Lake-enabled SparkSession."""
    _ensure_windows_hadoop_env()
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
