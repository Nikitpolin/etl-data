CREATE TABLE IF NOT EXISTS s_sql_dds.t_dm_task (
    fact_id BIGSERIAL PRIMARY KEY,
    customer_id INT REFERENCES s_sql_dds.t_dim_customer(customer_id),
    product_id INT REFERENCES s_sql_dds.t_dim_product(product_id),
    region_id INT REFERENCES s_sql_dds.t_dim_region(region_id),
    status_id INT REFERENCES s_sql_dds.t_dim_status(status_id),
    age INTEGER,
    salary NUMERIC(15,2),
    purchase_amount NUMERIC(15,2),
    transaction_count INTEGER,
    effective_from DATE,
    effective_to DATE,
    current_flag BOOLEAN,
    created_dt DATE DEFAULT CURRENT_DATE
);