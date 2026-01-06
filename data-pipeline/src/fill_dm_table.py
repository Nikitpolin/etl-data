import psycopg2
from config import DB_CONFIG

def fill_dm_table(start_dt=None, end_dt=None):
    """
    Заполняет витрину данных в PostgreSQL DWH
    """
    conn = None
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Starting DWH data load...")
        
        # Вызов функции загрузки данных в DWH
        if start_dt and end_dt:
            cursor.execute("SELECT s_sql_dds.fn_dm_data_load(%s, %s)", (start_dt, end_dt))
        else:
            cursor.execute("SELECT s_sql_dds.fn_dm_data_load(NULL, NULL)")
        
        # Коммит изменений
        conn.commit()
        
        # Проверка количества записей в фактовой таблице
        cursor.execute("SELECT COUNT(*) FROM s_sql_dds.t_dm_task")
        fact_count = cursor.fetchone()[0]
        
        # Проверка справочников
        cursor.execute("SELECT COUNT(*) FROM s_sql_dds.t_dim_customer")
        customer_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM s_sql_dds.t_dim_product")
        product_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM s_sql_dds.t_dim_region")
        region_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM s_sql_dds.t_dim_status")
        status_count = cursor.fetchone()[0]
        
        print(f"DWH data loaded successfully!")
        print(f"Fact records: {fact_count}")
        print(f"Dimensions - Customers: {customer_count}, Products: {product_count}, Regions: {region_count}, Statuses: {status_count}")
        
    except Exception as e:
        print(f"Error loading DWH data: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    fill_dm_table()