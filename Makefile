.PHONY: help up down status topic producer bronze silver gold init-db thrift reset test verify logs

help:
	@echo "=============================================================================="
	@echo "Real-Time Mobility Analytics Lakehouse Orchestrator (Makefile)"
	@echo "=============================================================================="
	@echo "make up         - Start all Docker containers"
	@echo "make down       - Stop and tear down all Docker containers"
	@echo "make status     - Check status of containers"
	@echo "make topic      - Create Kafka topic 'traffic-topic'"
	@echo "make producer   - Install dependencies and run traffic telemetry producer"
	@echo "make bronze     - Submit Bronze Spark Structured Streaming Job"
	@echo "make silver     - Submit Silver Spark Structured Streaming Job"
	@echo "make gold       - Submit Gold Spark Dimensional Modeling Job"
	@echo "make test       - Run automated test suite"
	@echo "make verify     - Run zero-Docker local end-to-end pipeline verification"
	@echo "make init-db    - Register Delta tables and BI views in Hive Metastore"
	@echo "make thrift     - Start Spark Thrift Server on port 10000"
	@echo "make reset      - Delete local warehouse tables and checkpoints"
	@echo "make logs       - Tail Docker container logs"
	@echo "=============================================================================="

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps

topic:
	bash ./scripts/01_create_kafka_topic.sh

producer:
	bash ./scripts/run_producer.sh

bronze:
	bash ./scripts/02_submit_bronze.sh

silver:
	bash ./scripts/03_submit_silver.sh

gold:
	bash ./scripts/04_submit_gold.sh

test:
	python -m unittest discover tests

verify:
	python scripts/verify_local.py

init-db:
	bash ./scripts/05_init_metastore.sh

thrift:
	bash ./scripts/06_start_thrift_server.sh

reset:
	@echo "WARNING: Deleting local warehouse tables and checkpoint directories..."
	rm -rf ./warehouse ./spark-ivy
	@echo "[SUCCESS] Local data reset cleanly."

logs:
	docker compose logs -f
