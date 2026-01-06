-- Целевая таблица в MySQL
CREATE TABLE IF NOT EXISTS t_dm_task (
    fact_id BIGINT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    region_id INT,
    status_id INT,
    age INT,
    salary DECIMAL(15,2),
    purchase_amount DECIMAL(15,2),
    transaction_count INT,
    effective_from DATE,
    effective_to DATE,
    current_flag BOOLEAN,
    created_dt DATE,
    INDEX idx_transaction_date (effective_from),
    INDEX idx_customer (customer_id)
);