up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

demo:
	python3 data_gen/generate_events.py
