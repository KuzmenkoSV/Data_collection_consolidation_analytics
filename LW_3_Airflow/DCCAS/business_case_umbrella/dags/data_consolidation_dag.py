from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd # type: ignore
import numpy as np

# Определение DAG
dag = DAG(
    'individual_exercize', 
    description='Var. 18. Consolidate data from CSV, Excel, JSON and save to Excel',
    schedule_interval='@once', 
    start_date=datetime(2024, 1, 1)
)

def extract_and_transform():
    # Пути к файлам данных
    csv_file = '/opt/airflow/dags/sales_2022.csv'
    excel_file = '/opt/airflow/dags/sales_2023.xlsx'
    json_file = '/opt/airflow/dags/products.json'

    #Шаг 1. Извлечение данных
    df_2022 = pd.read_csv(csv_file)
    df_2023 = pd.read_excel(excel_file)
    df_products = pd.read_json(json_file)

    #Шаг 2. Обработка пропущенных значений
    df_2022 = df_2022.dropna(subset=['date', 'product_id', 'quantity'])
    df_2022['sales'] = df_2022['sales'].fillna(df_2022['sales'].mean())
    df_2023 = df_2023.dropna(subset=['date', 'product_id', 'quantity'])
    df_2023['sales'] = df_2023['sales'].fillna(df_2023['sales'].mean())

    #Шаг 3. Приведение названия столбцов к единому формату
    df_2022.columns = df_2022.columns.str.lower()
    df_2023.columns = df_2023.columns.str.lower()
    df_products.columns = df_products.columns.str.lower()

    #Шаг 4. Объединение продаж за 2022 и 2023 год
    df_sales = pd.concat([df_2022, df_2023], ignore_index=True)

    #Шаг 5. Добавление информации о товарах
    df_consolidated = pd.merge(df_sales, df_products, on='product_id', how='left')

    #Шаг 6. Агрегация и анализ. Продажи по категориям и по годам
    df_consolidated['year'] = pd.to_datetime(df_consolidated['date']).dt.year
    sales_by_category = df_consolidated.groupby('category')['sales'].sum().sort_values(ascending=False)
    avg_sales_by_year = df_consolidated.groupby('year')['sales'].mean()

    #Шаг 7. Выгрузка в Excel
    with pd.ExcelWriter('resul_data.xlsx') as writer:
        # Записываем первый DataFrame на вкладку "Продажи по категориям"
        sales_by_category.to_excel(writer, sheet_name='Продажи по категориям', index=True)
        
        # Записываем второй DataFrame на вкладку "Продажи по годам"
        avg_sales_by_year.to_excel(writer, sheet_name='Продажи по годам', index=True)


# Определение задач
task_1 = DummyOperator(task_id="start", dag=dag)
task_2 = PythonOperator(task_id="ETL", python_callable=extract_and_transform, dag=dag)
task_3 = DummyOperator(task_id="end", dag=dag)

# Установка зависимостей между задачами
task_1 >> task_2 >> task_3