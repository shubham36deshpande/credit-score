from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple DAG for demonstration purposes',
    schedule_interval='@once',  # Set the schedule interval as needed
)

# Define a Python function to print a message
def print_message():
    print("Hello, this is a message printed by the print_message task!")

# Define a Python function to perform a simple operation
def perform_operation():
    result = 2 + 2
    print(f"The result of the operation is: {result}")

# Define two PythonOperator tasks
print_message_task = PythonOperator(
    task_id='print_message_task',
    python_callable=print_message,
    dag=dag,
)

perform_operation_task = PythonOperator(
    task_id='perform_operation_task',
    python_callable=perform_operation,
    dag=dag,
)

# Set the task dependencies
print_message_task >> perform_operation_task
