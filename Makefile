.PHONY: gen gen-large up init down logs psql ps

# Сгенерировать суточные партиции (грязные) под Airflow
gen:
	python generate/generate.py --scale small --dirty --partition-by-day --output ./data/raw

gen-large:
	python generate/generate.py --scale large --dirty --partition-by-day --output ./data/raw

init:            ## один раз: миграции метабазы + admin-пользователь
	docker compose up airflow-init

up:              ## поднять PostgreSQL + Airflow
	docker compose up -d

down:            ## остановить стек (данные сохраняются в volume)
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

ps:
	docker compose ps

psql:            ## psql в business-БД
	docker compose exec postgres psql -U postgres -d shop_mvp
