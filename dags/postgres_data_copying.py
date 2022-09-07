from datetime import datetime
from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

with DAG(
    dag_id="postgres_data_copying_dag",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1)    
) as dag:
    drop_user_table_x = PostgresOperator(
    task_id="delete_user_table_x",
    postgres_conn_id="POSTGRES_X",
    sql="sql/user_delete.sql",
    )
    create_user_table_in_x = PostgresOperator(
    task_id="create_user_table",
    postgres_conn_id="POSTGRES_X",
    sql="sql/user_schema.sql",
    )
    populate_user_table = PostgresOperator(
    task_id="populate_user_table",
    postgres_conn_id="POSTGRES_X",
    sql="sql/user_data.sql",
    )

    @task()
    def data_copying(**kwargs):
        src = PostgresHook(postgres_conn_id='POSTGRES_X')
        dest = PostgresHook(postgres_conn_id='POSTGRES_Y')
        src_conn = src.get_conn()
        src_cursor = src_conn.cursor()

        dest_conn = dest.get_conn()
        dest_cursor = dest_conn.cursor()

        data_from_source = src_cursor.execute("Select * from users;")
        
        dest.insert_rows(table="users", rows=src_cursor)
        dest_conn.commit()
        src_conn.close()
        dest_conn.close()

        return
    drop_user_table_y = PostgresOperator(
    task_id="delete_user_table_y",
    postgres_conn_id="POSTGRES_Y",
    sql="sql/user_delete.sql",
    )

    @task()
    def create_user_table_y(**kwargs):
        src = PostgresHook(postgres_conn_id='POSTGRES_X')
        dest = PostgresHook(postgres_conn_id='POSTGRES_Y')
        src_conn = src.get_conn()
        src_cursor = src_conn.cursor()

        dest_conn = dest.get_conn()
        dest_cursor = dest_conn.cursor()
        schema_query = """SELECT
            'CREATE TABLE if not exists ' || relname || E'\n(\n' ||
                array_to_string(
             array_agg(
             '    ' || column_name || ' ' ||  type || ' '|| not_null
            )
            , E',\n'
                ) || E'\n);\n'
            from
            (
            SELECT
                c.relname, a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
                case
                when a.attnotnull
                then 'NOT NULL'
                else 'NULL'
                END as not_null
            FROM pg_class c,
            pg_attribute a,
            pg_type t
            WHERE c.relname = 'users'
            AND a.attnum > 0
            AND a.attrelid = c.oid
            AND a.atttypid = t.oid
            ORDER BY a.attnum
            ) as tabledefinition
            group by relname;"""
        data_from_source = src_cursor.execute(schema_query)
        create_statement = src_cursor.fetchall()[0][0]
        dest_cursor.execute(create_statement)
        dest_conn.commit()
        src_conn.close()
        dest_conn.close()

    drop_user_table_x >> create_user_table_in_x >> populate_user_table >> drop_user_table_y >> create_user_table_y() >> data_copying()




    