# Instructions

The docker compose file is based on the official docker compose by airflow (https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). It uses the local executor to run the DAG. 

First task in the dag drops table `users` if table is present. The next task creates the `users` table is created on postgres x, and then data is inserted into it. After that the fourth task drops table `users` if it is present in postgres y.

Then the fifth task gets the table structure and details from postgres x and creates table `users` in postgres y. The final task copies data from postgres x to postgres y.

1. After cloning the repository, `cd` inside `glints_assignment`
2. Run `DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose up -d` (arm64 doesn't work properly with postgres)
3. The airflow UI should be available at http://127.0.0.1:5884/
4. Login using username : `airflow` and password : `airflow`
5. Please click on `postgres_data_copying_dag`. Then run the DAG by clicking the play icon under Actions column and select `Trigger DAG`.
6. The Postgres X is running on localhost port 5430
7. The Postgres Y is running on localhost port 5431
8. The postgres of airflow is running on localhost port 5432
9. Upon running the DAG, data is first populated in postgres x , in users table. Then it is copied to postgres y. The database name, username and password in both postgres x and y is `airflow`.