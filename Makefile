.PHONY: up down logs topics ps test-topics nuke

# Start all services
up:
	docker compose up -d

# Stop all services (keeps data)
down:
	docker compose down

# Tail all logs
logs:
	docker compose logs -f

# Manually run the topic creation script inside the Kafka container
topics:
	docker compose exec -T kafka bash /etc/kafka/create-topics.sh

# Show container status
ps:
	docker compose ps

# Test if topics exist, have correct partitions, and check message counts
test-topics:
	@echo "=== Existing Topics ==="
	@docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
	@echo ""
	@echo "=== Topic Stats (Partitions | Messages) ==="
	@for t in equities.prices crypto.trades forex.rates macro.indicators fundamentals.filings; do \
		partitions=$$(docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic $$t 2>/dev/null | head -1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $$2}'); \
		messages=$$(docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic $$t 2>/dev/null | awk -F: '{sum+=$$3} END{print sum+0}'); \
		printf "%-25s %3s partitions | %s messages\n" "$$t" "$$partitions" "$$messages"; \
	done

# DANGER: Wipes all data, fixes permissions, and starts fresh
nuke:
	@echo "Stopping stack..."
	docker compose down -v
	@echo "Wiping volumes..."
	sudo find volumes -mindepth 1 -delete
	@echo "Recreating directories & fixing ownership..."
	mkdir -p volumes/{zookeeper-data,zookeeper-log,kafka-data,grafana-data}
	sudo chown -R 1000:1000 volumes/zookeeper-data volumes/zookeeper-log volumes/kafka-data
	sudo chown -R 472:472   volumes/grafana-data
	@echo "Starting stack..."
	docker compose up -d
	@echo "Waiting 30s for healthchecks..."
	sleep 30
	@echo "Done. Run 'make test-topics' to verify."