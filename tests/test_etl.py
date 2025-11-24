import psycopg2
import sys
import os

# Добавляем путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_CONFIG
except ImportError:
    # Альтернативный способ если импорт не работает
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'database': 'etl_db',
        'user': 'user',
        'password': 'password'
    }

def init_database():
    #Инициализация базы данных - создание схемы, таблиц и функций
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Инициализация базы данных...")
        
        # Создание схемы
        print("Создание схемы s_sql_dds...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS s_sql_dds;")
        
        # Создание неструктурированной таблицы
        print("Создание таблицы t_sql_source_unstructured...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_unstructured (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50),
                user_name VARCHAR(100),
                age INTEGER,
                salary NUMERIC(15,2),
                purchase_amount NUMERIC(15,2),
                product_category VARCHAR(50),
                region VARCHAR(50),
                customer_status VARCHAR(20),
                transaction_count INTEGER,
                effective_from DATE,
                effective_to DATE,
                current_flag BOOLEAN,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Создание структурированной таблицы
        print("Создание таблицы t_sql_source_structured...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                user_name VARCHAR(100),
                age INTEGER,
                salary NUMERIC(15,2),
                purchase_amount NUMERIC(15,2),
                product_category VARCHAR(50),
                region VARCHAR(50),
                customer_status VARCHAR(20),
                transaction_count INTEGER,
                effective_from DATE,
                effective_to DATE,
                current_flag BOOLEAN,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Создание ТЕСТОВОЙ таблицы
        print("Создание тестовой таблицы t_sql_source_structured_copy...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured_copy (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                user_name VARCHAR(100),
                age INTEGER,
                salary NUMERIC(15,2),
                purchase_amount NUMERIC(15,2),
                product_category VARCHAR(50),
                region VARCHAR(50),
                customer_status VARCHAR(20),
                transaction_count INTEGER,
                effective_from DATE,
                effective_to DATE,
                current_flag BOOLEAN,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Создание ETL функции
        print("Создание функции fn_etl_data_load...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION s_sql_dds.fn_etl_data_load(
                start_date DATE DEFAULT '2023-01-01',
                end_date DATE DEFAULT '2023-12-31'
            )
            RETURNS INTEGER AS $$
            DECLARE
                processed_count INTEGER;
            BEGIN
                -- Очистка целевой таблицы в указанном диапазоне дат
                DELETE FROM s_sql_dds.t_sql_source_structured 
                WHERE effective_from >= start_date AND effective_to <= end_date;
                
                -- Вставка очищенных и трансформированных данных
                INSERT INTO s_sql_dds.t_sql_source_structured (
                    user_id, user_name, age, salary, purchase_amount, product_category,
                    region, customer_status, transaction_count, effective_from, effective_to, current_flag
                )
                SELECT 
                    user_id,
                    user_name,
                    -- Очистка возраста
                    CASE 
                        WHEN age IS NULL THEN 25
                        WHEN age < 18 THEN 18
                        WHEN age > 100 THEN 100
                        ELSE age
                    END AS age,
                    -- Очистка зарплаты
                    CASE 
                        WHEN salary < 0 THEN 0
                        WHEN salary > 1000000 THEN 1000000
                        ELSE ROUND(salary::NUMERIC, 2)
                    END AS salary,
                    -- Очистка суммы покупки
                    CASE 
                        WHEN purchase_amount < 0 THEN 0
                        WHEN purchase_amount > 100000 THEN 100000
                        ELSE ROUND(purchase_amount::NUMERIC, 2)
                    END AS purchase_amount,
                    -- Очистка категорий продуктов
                    CASE 
                        WHEN product_category NOT IN ('Electronics', 'Clothing', 'Books', 'Home', 'Sports') 
                        THEN 'Other'
                        ELSE product_category
                    END AS product_category,
                    region,
                    -- Стандартизация статусов
                    CASE 
                        WHEN customer_status IS NULL THEN 'unknown'
                        ELSE LOWER(customer_status)
                    END AS customer_status,
                    -- Очистка количества транзакций
                    CASE 
                        WHEN transaction_count < 0 THEN 0
                        WHEN transaction_count > 1000 THEN 1000
                        ELSE transaction_count
                    END AS transaction_count,
                    -- Корректировка дат
                    CASE 
                        WHEN effective_from < '2020-01-01' THEN '2023-01-01'::DATE
                        ELSE effective_from
                    END AS effective_from,
                    CASE 
                        WHEN effective_to < effective_from THEN effective_from + INTERVAL '30 days'
                        WHEN effective_to > '2024-12-31' THEN '2024-12-31'::DATE
                        ELSE effective_to
                    END AS effective_to,
                    current_flag
                FROM s_sql_dds.t_sql_source_unstructured
                WHERE effective_from >= start_date 
                    AND effective_to <= end_date
                    AND user_id IS NOT NULL;
                
                -- Получение количества обработанных записей
                GET DIAGNOSTICS processed_count = ROW_COUNT;
                
                RETURN processed_count;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Создание ТЕСТОВОЙ ETL функции
        print("Создание тестовой функции fn_etl_data_load_test...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION s_sql_dds.fn_etl_data_load_test(
                start_date DATE DEFAULT '2023-01-01',
                end_date DATE DEFAULT '2023-12-31'
            )
            RETURNS INTEGER AS $$
            DECLARE
                test_count INTEGER;
            BEGIN
                -- Очистка тестовой таблицы в указанном диапазоне дат
                DELETE FROM s_sql_dds.t_sql_source_structured_copy 
                WHERE effective_from >= start_date AND effective_to <= end_date;
                
                -- Копирование данных из структурированной таблицы в тестовую
                INSERT INTO s_sql_dds.t_sql_source_structured_copy 
                SELECT * FROM s_sql_dds.t_sql_source_structured
                WHERE effective_from >= start_date AND effective_to <= end_date;
                
                -- Получение количества скопированных записей
                GET DIAGNOSTICS test_count = ROW_COUNT;
                
                RETURN test_count;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        print("База данных успешно инициализирована!")
        
    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_database()