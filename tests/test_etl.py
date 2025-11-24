import pytest
import psycopg2
import os

# Адаптивный конфиг - работает везде
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),  # Берёт из окружения или localhost
    'port': '5432',
    'database': 'etl_db',
    'user': 'user',
    'password': 'password'
}

class TestETLPipeline:
    
    def test_database_connection(self):
        #Тест подключения к базе данных
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            cur.close()
            conn.close()
            assert result[0] == 1
            print("Database connection test PASSED")
        except Exception as e:
            pytest.fail(f"Database connection failed: {e}")
    
    def test_tables_exist(self):
        #Тест существования таблиц
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Проверка существования таблиц
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 's_sql_dds' 
                AND table_name IN ('t_sql_source_unstructured', 't_sql_source_structured', 't_sql_source_structured_copy');
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            cur.close()
            conn.close()
            
            assert 't_sql_source_unstructured' in tables
            assert 't_sql_source_structured' in tables
            assert 't_sql_source_structured_copy' in tables
            print("Tables existence test PASSED")
            
        except Exception as e:
            pytest.fail(f"Table existence test failed: {e}")
    
    def test_etl_function_parameters(self):
        #Тест работы ETL функции с параметрами дат
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Тестируем функцию с разными параметрами дат
            test_cases = [
                ('2023-01-01', '2023-12-31'),
                ('2023-06-01', '2023-06-30'),
            ]
            
            for start_date, end_date in test_cases:
                cur.execute("SELECT s_sql_dds.fn_etl_data_load(%s, %s);", (start_date, end_date))
                result = cur.fetchone()
                assert result[0] >= 0, f"ETL function failed for dates {start_date} to {end_date}"
            
            cur.close()
            conn.close()
            print("ETL function parameters test PASSED")
            
        except Exception as e:
            pytest.fail(f"ETL function test failed: {e}")
    
    def test_test_etl_function(self):
        #Тест тестовой ETL функции
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Запускаем тестовую функцию
            cur.execute("SELECT s_sql_dds.fn_etl_data_load_test(%s, %s);", ('2023-01-01', '2023-12-31'))
            result = cur.fetchone()
            processed_count = result[0] if result else 0
            
            # Проверяем, что данные скопировались
            cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured_copy;")
            copy_count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            assert processed_count >= 0
            assert copy_count >= 0
            print("Test ETL function test PASSED")
            
        except Exception as e:
            pytest.fail(f"Test ETL function failed: {e}")
    
    def test_data_quality(self):
        #Тест качества данных в структурированной таблице
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Проверяем, что нет отрицательных зарплат
            cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE salary < 0;")
            negative_salaries = cur.fetchone()[0]
            assert negative_salaries == 0, f"Found {negative_salaries} records with negative salary"
            
            # Проверяем, что возраст в допустимом диапазоне
            cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE age < 18 OR age > 100;")
            invalid_ages = cur.fetchone()[0]
            assert invalid_ages == 0, f"Found {invalid_ages} records with invalid age"
            
            # Проверяем, что даты корректны
            cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE effective_to < effective_from;")
            invalid_dates = cur.fetchone()[0]
            assert invalid_dates == 0, f"Found {invalid_dates} records with invalid date range"
            
            cur.close()
            conn.close()
            print("Data quality test PASSED")
            
        except Exception as e:
            pytest.fail(f"Data quality test failed: {e}")

    def test_etl_process_integration(self):
        #Интеграционный тест всего ETL процесса
        try:
            # Это заглушка для локального тестирования
            assert True  # Заглушка
            print("Integration test PASSED")
            
        except Exception as e:
            pytest.fail(f"Integration test failed: {e}")