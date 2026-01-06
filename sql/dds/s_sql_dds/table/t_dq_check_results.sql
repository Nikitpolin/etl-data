-- Таблица для хранения результатов проверок качества данных
DROP TABLE IF EXISTS s_sql_dds.t_dq_check_results;

CREATE TABLE s_sql_dds.t_dq_check_results (
    check_id SERIAL PRIMARY KEY,
    check_type VARCHAR(50),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    check_name VARCHAR(200),
    execution_date TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    expected_value NUMERIC,
    actual_value NUMERIC,
    error_threshold NUMERIC,
    error_message VARCHAR(500)
);

-- Индексы для быстрого поиска
CREATE INDEX idx_dq_check_date ON s_sql_dds.t_dq_check_results(execution_date);
CREATE INDEX idx_dq_check_status ON s_sql_dds.t_dq_check_results(status);