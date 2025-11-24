import psycopg2
from config import DB_CONFIG  # Убрали src.

def fill_structured_table(start_date='2023-01-01', end_date='2023-12-31'):
    #Запуск SQL-функции для очистки данных и загрузки в структурированную таблицу
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Вызов SQL-функции для ETL
        cur.execute("SELECT s_sql_dds.fn_etl_data_load(%s, %s);", (start_date, end_date))
        
        # Получение количества обработанных записей
        result = cur.fetchone()
        processed_count = result[0] if result else 0
        
        conn.commit()
        print(f"Успешно обработано {processed_count} записей в t_sql_source_structured")
        
    except Exception as e:
        print(f"Ошибка при выполнении ETL: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()