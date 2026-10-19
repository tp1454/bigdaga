.PHONY: up down logs topics ps

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

topics:
	docker compose exec -T kafka bash /etc/kafka/create-topics.sh

ps:
	docker compose ps
