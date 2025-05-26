from airflow.decorators import dag, task
from datetime import datetime
import random

@dag(
    start_date=datetime(2025, 5,26),
    schedule='@daily',
    catchup=False,  # only run from now on, don't backfill
    description = 'A simple DAG to generate and check random number using TaskFlow API',
    tags=['random_number']
)
def random_number():
    @task
    def generate_random_number():
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number

    @task
    def check_even_odd(number: int):
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")
    
    check_even_odd(generate_random_number())

random_number()
