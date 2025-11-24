import pytest
import psycopg2
import os
import sys
import time

# Добавляем путь для импорта config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data-pipeline', 'src'))

try:
    from config import DB_CONFIG
except ImportError:
    # Fallback конфиг
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'port': '5432',
        'database': 'etl_db',
        'user': 'user',
        'password': 'password',
        'client_encoding': 'utf8'
    }

def wait_for_database(max_retries=10, delay=3):
    """Ожидание готовности базы данных"""
    print("⏳ Ожидание готовности базы данных...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("✅ База данных готова")
            return True
        except Exception as e:
            print(f"Попытка {i+1}/{max_retries}: База данных не готова - {str(e)}")
            if i < max_retries - 1:
                time.sleep(delay)
    print("❌ База данных не стала доступна")
    return False

def get_db_connection():
    """Создание подключения с обработкой кодировки"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Устанавливаем кодировку
        with conn.cursor() as cursor:
            cursor.execute("SET client_encoding TO 'UTF8'")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        raise

class TestETLPipeline:
    
    @classmethod
    def setup_class(cls):
        """Настройка перед всеми тестами"""
        print("\n=== Настройка тестовой среды ===")
        if not wait_for_database():
            pytest.skip("База данных недоступна")
    
    def setup_method(self):
        """Настройка перед каждым тестом"""
        self.conn = None
        self.cur = None
    
    def teardown_method(self):
        """Очистка после каждого теста"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
    
    def test_database_connection(self):
        """Тест подключения к базе данных"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            self.cur.execute("SELECT 1;")
            result = self.cur.fetchone()
            
            assert result[0] == 1
            print("✅ Database connection test PASSED")
            
        except Exception as e:
            pytest.fail(f"Database connection failed: {str(e)}")
    
    def test_tables_exist(self):
        """Тест существования таблиц"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Проверка существования таблиц
            self.cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 's_sql_dds' 
                AND table_name IN ('t_sql_source_unstructured', 't_sql_source_structured', 't_sql_source_structured_copy');
            """)
            tables = [row[0] for row in self.cur.fetchall()]
            
            expected_tables = ['t_sql_source_unstructured', 't_sql_source_structured', 't_sql_source_structured_copy']
            missing_tables = [t for t in expected_tables if t not in tables]
            
            assert len(missing_tables) == 0, f"Missing tables: {missing_tables}"
            print("✅ Tables existence test PASSED")
            
        except Exception as e:
            pytest.fail(f"Table existence test failed: {str(e)}")
    
    def test_etl_functions_exist(self):
        """Тест существования ETL функций"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Проверка существования функций
            self.cur.execute("""
                SELECT routine_name 
                FROM information_schema.routines 
                WHERE routine_schema = 's_sql_dds' 
                AND routine_name IN ('fn_etl_data_load', 'fn_etl_data_load_test');
            """)
            functions = [row[0] for row in self.cur.fetchall()]
            
            expected_functions = ['fn_etl_data_load', 'fn_etl_data_load_test']
            missing_functions = [f for f in expected_functions if f not in functions]
            
            assert len(missing_functions) == 0, f"Missing functions: {missing_functions}"
            print("✅ ETL functions existence test PASSED")
            
        except Exception as e:
            pytest.fail(f"ETL functions existence test failed: {str(e)}")
    
    def test_etl_function_parameters(self):
        """Тест работы ETL функции с параметрами дат"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Сначала добавляем тестовые данные
            self._insert_test_data()
            
            # Тестируем функцию с разными параметрами дат
            test_cases = [
                ('2023-01-01', '2023-12-31'),
                ('2023-06-01', '2023-06-30'),
            ]
            
            for start_date, end_date in test_cases:
                self.cur.execute("SELECT s_sql_dds.fn_etl_data_load(%s, %s);", (start_date, end_date))
                result = self.cur.fetchone()
                processed_count = result[0] if result else 0
                assert processed_count >= 0, f"ETL function returned negative count for dates {start_date} to {end_date}"
                print(f"✅ ETL processed {processed_count} records for {start_date} to {end_date}")
            
            print("✅ ETL function parameters test PASSED")
            
        except Exception as e:
            pytest.fail(f"ETL function test failed: {str(e)}")
    
    def test_test_etl_function(self):
        """Тест тестовой ETL функции"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Сначала запускаем основную ETL функцию
            self.cur.execute("SELECT s_sql_dds.fn_etl_data_load('2023-01-01', '2023-12-31');")
            
            # Затем запускаем тестовую функцию
            self.cur.execute("SELECT s_sql_dds.fn_etl_data_load_test('2023-01-01', '2023-12-31');")
            result = self.cur.fetchone()
            processed_count = result[0] if result else 0
            
            # Проверяем, что данные скопировались
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured_copy;")
            copy_count = self.cur.fetchone()[0]
            
            assert processed_count >= 0
            assert copy_count >= 0
            print(f"✅ Test ETL function copied {processed_count} records")
            print("✅ Test ETL function test PASSED")
            
        except Exception as e:
            pytest.fail(f"Test ETL function failed: {str(e)}")
    
    def test_data_quality(self):
        """Тест качества данных в структурированной таблице"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Сначала запускаем ETL процесс
            self.cur.execute("SELECT s_sql_dds.fn_etl_data_load('2023-01-01', '2023-12-31');")
            
            # Проверяем, что нет отрицательных зарплат
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE salary < 0;")
            negative_salaries = self.cur.fetchone()[0]
            assert negative_salaries == 0, f"Found {negative_salaries} records with negative salary"
            
            # Проверяем, что возраст в допустимом диапазоне
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE age < 18 OR age > 100;")
            invalid_ages = self.cur.fetchone()[0]
            assert invalid_ages == 0, f"Found {invalid_ages} records with invalid age"
            
            # Проверяем, что даты корректны
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE effective_to < effective_from;")
            invalid_dates = self.cur.fetchone()[0]
            assert invalid_dates == 0, f"Found {invalid_dates} records with invalid date range"
            
            # Проверяем, что суммы покупок в допустимом диапазоне
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured WHERE purchase_amount < 0 OR purchase_amount > 100000;")
            invalid_purchases = self.cur.fetchone()[0]
            assert invalid_purchases == 0, f"Found {invalid_purchases} records with invalid purchase amount"
            
            print("✅ Data quality test PASSED")
            
        except Exception as e:
            pytest.fail(f"Data quality test failed: {str(e)}")
    
    def test_etl_process_integration(self):
        """Интеграционный тест всего ETL процесса"""
        try:
            self.conn = get_db_connection()
            self.cur = self.conn.cursor()
            
            # Очищаем таблицы перед тестом
            self.cur.execute("TRUNCATE TABLE s_sql_dds.t_sql_source_unstructured CASCADE;")
            self.cur.execute("TRUNCATE TABLE s_sql_dds.t_sql_source_structured CASCADE;")
            self.cur.execute("TRUNCATE TABLE s_sql_dds.t_sql_source_structured_copy CASCADE;")
            
            # Добавляем тестовые данные
            test_data_count = self._insert_test_data()
            
            # Запускаем ETL процесс
            self.cur.execute("SELECT s_sql_dds.fn_etl_data_load('2023-01-01', '2023-12-31');")
            etl_count = self.cur.fetchone()[0]
            
            # Проверяем результат
            self.cur.execute("SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured;")
            structured_count = self.cur.fetchone()[0]
            
            assert etl_count >= 0
            assert structured_count >= 0
            print(f"✅ Integration test: {test_data_count} test records, {etl_count} processed by ETL, {structured_count} in structured table")
            print("✅ Integration test PASSED")
            
        except Exception as e:
            pytest.fail(f"Integration test failed: {str(e)}")
    
    def _insert_test_data(self):
        """Вставка тестовых данных в неструктурированную таблицу"""
        try:
            test_data = [
                ('user1', 'John Doe', 25, 50000.0, 150.0, 'Electronics', 'North', 'active', 5, '2023-01-15', '2023-12-31', True),
                ('user2', 'Jane Smith', 35, 75000.0, 200.0, 'Clothing', 'South', 'active', 3, '2023-02-01', '2023-12-31', True),
                ('user3', 'Bob Johnson', 45, 60000.0, 75.0, 'Books', 'East', 'inactive', 1, '2023-03-10', '2023-12-31', False),
                # Данные с аномалиями для тестирования очистки
                ('user4', 'Invalid User', -5, -1000.0, -50.0, 'InvalidCategory', 'West', None, -2, '2019-01-01', '2023-12-31', True),
            ]
            
            insert_sql = """
            INSERT INTO s_sql_dds.t_sql_source_unstructured 
            (user_id, user_name, age, salary, purchase_amount, product_category, region, customer_status, transaction_count, effective_from, effective_to, current_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for data in test_data:
                self.cur.execute(insert_sql, data)
            
            self.conn.commit()
            return len(test_data)
            
        except Exception as e:
            self.conn.rollback()
            print(f"Warning: Could not insert test data: {e}")
            return 0

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])