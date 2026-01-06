import psycopg2
import pymysql
from config import PG_CONFIG, MYSQL_CONFIG

def migrate_to_mysql(start_dt=None, end_dt=None):
    
    # Мигрирует данные из PostgreSQL в MySQL используя pymysql
    
    pg_conn = None
    mysql_conn = None
    
    try:
        # Подключение к PostgreSQL
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor()
        
        # Подключение к MySQL через pymysql
        print("Connecting to MySQL...")
        mysql_conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            charset='utf8mb4'
        )
        mysql_cursor = mysql_conn.cursor()
        
        # Выборка данных из представления PostgreSQL
        print("Fetching data from PostgreSQL DWH...")
        query = """
            SELECT fact_id, customer_id, product_id, region_id, status_id,
                   age, salary, purchase_amount, transaction_count,
                   effective_from, effective_to, current_flag, created_dt
            FROM s_sql_dds.v_dm_task
            WHERE (%s IS NULL OR effective_from >= %s)
              AND (%s IS NULL OR effective_to <= %s)
        """
        
        pg_cursor.execute(query, (start_dt, start_dt, end_dt, end_dt))
        data = pg_cursor.fetchall()
        
        print(f"Fetched {len(data)} records from PostgreSQL DWH")
        
        if len(data) == 0:
            print("No data to migrate!")
            return
        
        # Очистка staging таблицы в MySQL
        print("Cleaning MySQL staging table...")
        mysql_cursor.execute("DELETE FROM t_dm_stg_task")
        
        # Вставка данных в MySQL staging таблицу
        print("Inserting data into MySQL staging table...")
        insert_query = """
            INSERT INTO t_dm_stg_task 
            (fact_id, customer_id, product_id, region_id, status_id,
             age, salary, purchase_amount, transaction_count,
             effective_from, effective_to, current_flag, created_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        mysql_cursor.executemany(insert_query, data)
        mysql_conn.commit()
        
        print(f"Inserted {mysql_cursor.rowcount} records into MySQL staging table")
        
        # Вызов процедуры загрузки в целевую таблицу MySQL
        print("Loading data to MySQL target table...")
        
        # Для pymysql используем execute для вызова процедуры
        call_query = "CALL fn_dm_data_stg_to_dm_load(%s, %s)"
        mysql_cursor.execute(call_query, (start_dt, end_dt))
        
        # Получение результата процедуры
        result = mysql_cursor.fetchone()
        if result:
            print(f"MySQL procedure result: {result[0]}")
        
        mysql_conn.commit()
        
        # Проверка финального количества записей
        mysql_cursor.execute("SELECT COUNT(*) FROM t_dm_task")
        final_count = mysql_cursor.fetchone()[0]
        
        print(f"Data migration completed successfully!")
        print(f"Total records in MySQL DWH: {final_count}")
        
    except Exception as e:
        print(f"Error migrating data: {e}")
        if mysql_conn:
            mysql_conn.rollback()
        raise
    finally:
        if pg_conn:
            pg_conn.close()
        if mysql_conn:
            mysql_conn.close()

if __name__ == "__main__":
    migrate_to_mysql()