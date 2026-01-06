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

def run_data_quality_checks(start_dt=None, end_dt=None):
    """
    Запускает проверки качества данных
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Запуск проверок качества данных...")
        
        # Запуск функции проверки качества данных
        if start_dt and end_dt:
            cursor.execute("SELECT s_sql_dds.fn_dq_checks_load(%s, %s)", (start_dt, end_dt))
        else:
            cursor.execute("SELECT s_sql_dds.fn_dq_checks_load(NULL, NULL)")
        
        conn.commit()
        
        # Получаем результаты
        cursor.execute("""
            SELECT check_name, status, error_message, execution_date
            FROM s_sql_dds.t_dq_check_results
            WHERE execution_date = (
                SELECT MAX(execution_date) 
                FROM s_sql_dds.t_dq_check_results
            )
            ORDER BY check_id
        """)
        
        results = cursor.fetchall()
        
        
        passed = 0
        failed = 0
        errors = 0
        
        for result in results:
            check_name, status, error_message, exec_date = result
            if status == 'passed':
                passed += 1
                print(f"[УСПЕХ] {check_name}: {status}")
            elif status == 'failed':
                failed += 1
                print(f"[НЕУДАЧА] {check_name}: {status} - {error_message}")
            elif status == 'error':
                errors += 1
                print(f"[ОШИБКА] {check_name}: {status} - {error_message}")
            else:
                print(f"[ИНФО] {check_name}: {status}")
        
        print(f"Итог: Всего: {len(results)}, Успешно: {passed}, Неудачно: {failed}, Ошибок: {errors}")
        
        if failed == 0 and errors == 0:
            print("Все проверки качества данных пройдены успешно!")
        else:
            print(f"Обнаружены проблемы качества данных: {failed} неудачных проверок, {errors} ошибок")
        
        cursor.close()
        
    except Exception as e:
        print(f"Ошибка при запуске проверок качества данных: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_data_quality_checks()