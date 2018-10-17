from __future__ import print_function
from builtins import range
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import time
from pprint import pprint

args = {
    'owner': 'moe',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='python_operator', default_args=args, schedule_interval=None)


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever'

run_this = PythonOperator(
    task_id='print_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)


def sleep_function(randombase):
    time.sleep(randombase)

for i in range(5):
    task = PythonOperator(
        task_id='sleep_for_'+ str(i),
        python_callable=sleep_function,
        op_kwargs={'randombase': float(i) / 10},
        dag=dag)
    task.set_upstream(run_this)