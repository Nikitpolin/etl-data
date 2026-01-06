-- Справочник клиентов
CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) UNIQUE NOT NULL,
    created_dt DATE DEFAULT CURRENT_DATE
);

-- Справочник продуктов
CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_product (
    product_id SERIAL PRIMARY KEY,
    product_category VARCHAR(100) UNIQUE NOT NULL,
    created_dt DATE DEFAULT CURRENT_DATE
);

-- Справочник регионов
CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_region (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) UNIQUE NOT NULL,
    created_dt DATE DEFAULT CURRENT_DATE
);

-- Справочник статусов
CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_status (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL,
    created_dt DATE DEFAULT CURRENT_DATE
);