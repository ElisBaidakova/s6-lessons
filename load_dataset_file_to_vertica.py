import contextlib
import hashlib
import json
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='vertica.data-engineer.education-services.ru',
        port=5433,
        user='vt260419031e56',
        password = '65b93d5fdb024f46aba54fd6b55531d8'
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/users.csv',
            'schema': 'VT260419031E56__STAGING',
            'table': 'users',
            'columns': ['id', 'chat_name', 'registration_dt', 'country', 'age'],
            'type_override': {'age': 'Int64'},
        },
    )

    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/groups.csv',
            'schema': 'VT260419031E56__STAGING',
            'table': 'groups',
            'columns': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'],
        },
    )

    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/dialogs.csv',
            'schema': 'VT260419031E56__STAGING',
            'table': 'dialogs',
            'columns': ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group'],
            'type_override': {'message_group': 'Int64'},
        },
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'VT260419031E56__STAGING',
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
        },
    )

    start >> [load_users, load_groups, load_dialogs, load_group_log] >> end


_ = sprint6_dag_load_data_to_staging()
