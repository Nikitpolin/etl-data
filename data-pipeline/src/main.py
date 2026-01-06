import sys
from etl import etl
from fill_dm_table import fill_dm_table
from migrate_to_mysql import migrate_to_mysql

def main():
    """
    Основная функция запуска всего пайплайна
    """
    try:
        # Проверяем наличие флага --skip-mysql в аргументах командной строки
        skip_mysql = '--skip-mysql' in sys.argv
        
        print("=== Starting Complete Data Pipeline ===")
        
        # Шаг 1: ETL процесс (из первой лабы)
        print("\n1. Running ETL process...")
        etl()
        
        # Шаг 2: Заполнение DWH в PostgreSQL
        print("\n2. Loading data to PostgreSQL DWH...")
        fill_dm_table()
        
        # Шаг 3: Миграция в MySQL (с обработкой ошибок)
        if not skip_mysql:
            print("\n3. Migrating data to MySQL DWH...")
            try:
                migrate_to_mysql()
            except Exception as e:
                print(f"Warning: MySQL migration failed - {e}")
                print("Continuing pipeline without MySQL migration...")
        else:
            print("\n3. Skipping MySQL migration (--skip-mysql flag set)")
        
        print("\n=== Complete Pipeline finished successfully ===")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()