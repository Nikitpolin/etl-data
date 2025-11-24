import psycopg2
import pandas as pd  # Добавьте этот импорт
from config import DB_CONFIG

def load_data_to_db(df):
    #Загрузка данных в неструктурированную таблицу PostgreSQL
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Очистка таблицы перед загрузкой новых данных
        cur.execute("TRUNCATE TABLE s_sql_dds.t_sql_source_unstructured;")
        
        print("Загрузка данных...")
        
        # Подготовка данных для вставки с обработкой исключений
        successful_inserts = 0
        for index, row in df.iterrows():
            try:
                # Преобразование NaN в None для PostgreSQL
                age = row['age'] if pd.notna(row['age']) else None
                salary = float(row['salary']) if pd.notna(row['salary']) else None
                purchase_amount = float(row['purchase_amount']) if pd.notna(row['purchase_amount']) else None
                transaction_count = row['transaction_count'] if pd.notna(row['transaction_count']) else None
                
                cur.execute("""
                    INSERT INTO s_sql_dds.t_sql_source_unstructured 
                    (user_id, user_name, age, salary, purchase_amount, product_category, 
                     region, customer_status, transaction_count, effective_from, effective_to, current_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['user_id'], 
                    row['user_name'], 
                    age, 
                    salary, 
                    purchase_amount, 
                    row['product_category'], 
                    row['region'],
                    row['customer_status'], 
                    transaction_count, 
                    row['effective_from'],
                    row['effective_to'], 
                    row['current_flag']
                ))
                successful_inserts += 1
                
            except Exception as e:
                print(f"Ошибка при вставке строки {index}: {e}")
                print(f"Проблемные данные: {row.to_dict()}")
                continue  # Продолжаем со следующей строкой
        
        conn.commit()
        print(f"Успешно загружено {successful_inserts} из {len(df)} записей в t_sql_source_unstructured")
        
        return successful_inserts
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 0
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()