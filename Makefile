.PHONY: help pipeline-up pipeline-run pipeline-read pipeline-down clean

help:
	@echo "Targets:"
	@echo "  make pipeline-up   - start Trino + MinIO + dbt containers"
	@echo "  make pipeline-run  - run dbt -> Trino -> S3 (Delta-only)"
	@echo "  make pipeline-read - read transformed Delta table through Trino"
	@echo "  make pipeline-down - stop containers"
	@echo "  make clean         - remove local generated artifacts"

pipeline-up:
	docker compose -f infra/docker-compose.trino.yml up -d --remove-orphans

pipeline-run:
	./scripts/run_trino_dbt_pipeline.sh

pipeline-read:
	./scripts/read_trino_table.sh

pipeline-down:
	docker compose -f infra/docker-compose.trino.yml down

clean:
	rm -rf data_lake notebooks/data_lake __pycache__ .pytest_cache
