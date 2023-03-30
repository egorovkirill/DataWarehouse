.PHONY: build down

build:
	docker-compose up -d
down:
	docker-compose down
	rm -r db/.database
	rm -r vertica_data/vertica
	rm -r dags/__pycache__
	
hash:
	docker-compose run migrate migrate hash

migrate:
	docker-compose run migrate migrate apply 1 --url "postgresql://postgres:postgres@db:5432/postgres?search_path=public&sslmode=disable"

status:
	docker-compose run migrate migrate status --url "postgresql://postgres:postgres@db:5432/postgres?search_path=public&sslmode=disable"

