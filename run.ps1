# ==============================================================================
# run.ps1 - Windows PowerShell Orchestrator for Real-Time Mobility Lakehouse
# ==============================================================================
param (
    [Parameter(Mandatory=$false, Position=0)]
    [ValidateSet("up", "down", "status", "topic", "producer", "bronze", "silver", "gold", "test", "verify", "init-db", "thrift", "reset", "logs", "help")]
    [string]$Command = "help",

    [Parameter(Mandatory=$false)]
    [switch]$Force
)

function Show-Help {
    Write-Host @"
==============================================================================
Real-Time Mobility Analytics Lakehouse Orchestrator (PowerShell)
==============================================================================
Usage: .\run.ps1 <command> [-Force]

Available Commands:
  up          - Start all Docker containers (Kafka, Spark, Hive Metastore, Postgres)
  down        - Stop and tear down all Docker containers
  status      - Display container health and running processes
  topic       - Create Kafka topic 'traffic-topic' (3 partitions)
  producer    - Install requirements and run the traffic telemetry producer
  bronze      - Submit Bronze layer Spark streaming job
  silver      - Submit Silver layer Spark streaming job
  gold        - Submit Gold layer Spark streaming job
  test        - Run automated unit and integration test suite
  verify      - Run zero-Docker local end-to-end pipeline verification
  init-db     - Initialize Hive Metastore database, Delta tables, and BI views
  thrift      - Start Spark Thrift Server on port 10000 for Power BI
  reset       - Clean only generated warehouse and checkpoint data (with warning)
  logs        - View Docker compose logs
  help        - Display this help message
==============================================================================
"@
}

switch ($Command) {
    "up" {
        Write-Host "[INFO] Starting Docker containers..." -ForegroundColor Cyan
        docker compose up -d
    }
    "down" {
        Write-Host "[INFO] Stopping Docker containers..." -ForegroundColor Yellow
        docker compose down
    }
    "status" {
        docker compose ps
    }
    "topic" {
        Write-Host "[INFO] Creating Kafka topic 'traffic-topic'..." -ForegroundColor Cyan
        docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists
    }
    "producer" {
        Write-Host "[INFO] Installing requirements and launching traffic telemetry producer..." -ForegroundColor Cyan
        pip install -r .\producer\requirements.txt
        python .\producer\traffic_dirty_producer.py
    }
    "bronze" {
        Write-Host "[INFO] Submitting Bronze Spark Streaming Job..." -ForegroundColor Cyan
        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_bronze.py
    }
    "silver" {
        Write-Host "[INFO] Submitting Silver Spark Streaming Job..." -ForegroundColor Cyan
        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_silver.py
    }
    "gold" {
        Write-Host "[INFO] Submitting Gold Spark Streaming Job..." -ForegroundColor Cyan
        docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_gold.py
    }
    "test" {
        Write-Host "[INFO] Running automated test suite..." -ForegroundColor Cyan
        if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
        $env:PATH = "$env:HADOOP_HOME\bin;" + $env:PATH
        $env:SPARK_HOME = "C:\Users\amith\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark"
        python -m unittest discover tests
    }
    "verify" {
        Write-Host "[INFO] Running zero-Docker local end-to-end pipeline verification..." -ForegroundColor Cyan
        if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
        $env:PATH = "$env:HADOOP_HOME\bin;" + $env:PATH
        $env:SPARK_HOME = "C:\Users\amith\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark"
        python scripts\verify_local.py
    }
    "init-db" {
        Write-Host "[INFO] Initializing Hive Metastore tables and BI views..." -ForegroundColor Cyan
        docker exec -i spark-worker /opt/spark/bin/spark-sql --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=/opt/spark/warehouse -f /opt/spark-sql/init_all.sql
    }
    "thrift" {
        Write-Host "[INFO] Starting Spark Thrift Server on port 10000 with Delta packages..." -ForegroundColor Cyan
        docker exec -it spark-worker /opt/spark/sbin/start-thriftserver.sh --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=/opt/spark/warehouse
    }
    "reset" {
        Write-Warning "WARNING: This will permanently delete local warehouse Delta tables and streaming checkpoints!"
        $proceed = $Force
        if (-not $proceed) {
            $response = Read-Host "Are you sure you want to delete all local warehouse/checkpoint data? (yes/no)"
            $proceed = ($response -eq "yes")
        }
        if ($proceed) {
            if (Test-Path ".\warehouse") {
                Remove-Item -Recurse -Force ".\warehouse"
                Write-Host "[INFO] Removed .\warehouse" -ForegroundColor Yellow
            }
            if (Test-Path ".\spark-ivy") {
                Remove-Item -Recurse -Force ".\spark-ivy"
                Write-Host "[INFO] Removed .\spark-ivy" -ForegroundColor Yellow
            }
            Write-Host "[SUCCESS] Local generated data reset cleanly." -ForegroundColor Green
        } else {
            Write-Host "[ABORTED] Reset cancelled by user." -ForegroundColor Yellow
        }
    }
    "logs" {
        docker compose logs -f
    }
    Default {
        Show-Help
    }
}
