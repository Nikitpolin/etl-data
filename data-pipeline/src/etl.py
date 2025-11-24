from get_dataset import get_dataset
from load_data_to_db import load_data_to_db
from fill_structured_table import fill_structured_table
from init_database import init_database

def etl():

    #Верхнеуровневая ETL-функция без параметров
    print("Запуск ETL процесса...")
    
    # 0. Инициализация базы данных
    print("Этап 0: Инициализация базы данных")
    init_database()
    
    # 1. Генерация данных
    print("Этап 1: Генерация синтетических данных")
    df = get_dataset(rows=1000)  # Уменьшим до 1000 для тестирования
    print(f"Сгенерировано {len(df)} записей")
    
    # 2. Загрузка в неструктурированную таблицу
    print("Этап 2: Загрузка в неструктурированную таблицу")
    loaded_count = load_data_to_db(df)
    
    if loaded_count > 0:
        # 3. Очистка и загрузка в структурированную таблицу
        print("Этап 3: Очистка и трансформация данных")
        fill_structured_table(start_date='2023-01-01', end_date='2023-12-31')
        print("ETL процесс завершен успешно!")
    else:
        print("ETL процесс завершен с ошибками - данные не были загружены")