# Instructions

1. After cloning the repository, `cd` inside `glints_assignment`
2. Run `DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose up -d` (arm64 doesn't work properly with postgres)
3. The airflow UI should be available at http://127.0.0.1:5884/
4. Please click on `postgres_data_copying_dag`. Then run the DAG by clicking the play icon under Actions column and select `Trigger DAG`.
5. The Postgres X is running on localhost port 5430
6. The Postgres Y is running on localhost port 5431
7. The postgres of airflow is running on localhost port 5432
8. Upon running the DAG, data is first populated in postgres x , in users table. Then it is copied to postgres y.