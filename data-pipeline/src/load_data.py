import psycopg2
import pandas as pd
from get_dataset import get_dataset

print("Генерация и загрузка тестовых данных...")

# Генерируем данные
df = get_dataset(rows=500)
print(f"Сгенерировано {len(df)} записей")

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="etl_db",
    user="user",
    password="password"
)

cur = conn.cursor()

# Очистка таблицы
cur.execute("DELETE FROM s_sql_dds.t_sql_source_unstructured;")

# Загрузка данных
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO s_sql_dds.t_sql_source_unstructured 
        (user_id, user_name, age, salary, purchase_amount, product_category, 
         region, customer_status, transaction_count, effective_from, effective_to, current_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['user_id'], row['user_name'], row['age'], 
        float(row['salary']), float(row['purchase_amount']), 
        row['product_category'], row['region'], row['customer_status'],
        int(row['transaction_count']), row['effective_from'], 
        row['effective_to'], row['current_flag']
    ))

conn.commit()
print(f"Загружено {cur.rowcount} записей в t_sql_source_unstructured")

cur.close()
conn.close()