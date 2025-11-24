-- Создание схемы
CREATE SCHEMA IF NOT EXISTS s_sql_dds;

-- Создание неструктурированной таблицы
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

-- Создание структурированной таблицы
CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    user_name VARCHAR(100),
    age INTEGER CHECK (age BETWEEN 18 AND 100),
    salary NUMERIC(15,2) CHECK (salary >= 0),
    purchase_amount NUMERIC(15,2) CHECK (purchase_amount >= 0 AND purchase_amount <= 100000),
    product_category VARCHAR(50) CHECK (product_category IN ('Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Other')),
    region VARCHAR(50),
    customer_status VARCHAR(20),
    transaction_count INTEGER CHECK (transaction_count >= 0),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    current_flag BOOLEAN,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (effective_to >= effective_from)
);

-- ⭐⭐ ДОБАВИТЬ: ТЕСТОВАЯ ТАБЛИЦА ДЛЯ АВТОТЕСТОВ ⭐⭐
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

CREATE INDEX IF NOT EXISTS idx_structured_user_id ON s_sql_dds.t_sql_source_structured(user_id);
CREATE INDEX IF NOT EXISTS idx_structured_dates ON s_sql_dds.t_sql_source_structured(effective_from, effective_to);

-- Функция для ETL процесса
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
        -- Очистка возраста: замена NULL, ограничение диапазона
        CASE 
            WHEN age IS NULL THEN 25
            WHEN age < 18 THEN 18
            WHEN age > 100 THEN 100
            ELSE age
        END AS age,
        -- Очистка зарплаты: замена отрицательных значений, ограничение аномалий
        CASE 
            WHEN salary < 0 THEN 0
            WHEN salary > 1000000 THEN 1000000
            ELSE ROUND(salary::NUMERIC, 2)
        END AS salary,
        -- Очистка суммы покупки: замена отрицательных, ограничение выбросов
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

-- ⭐⭐ ДОБАВИТЬ: ТЕСТОВАЯ ФУНКЦИЯ ДЛЯ АВТОТЕСТОВ ⭐⭐
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