CREATE OR REPLACE VIEW s_sql_dds.v_dm_task AS
SELECT 
    fact_id,
    customer_id,
    product_id,
    region_id,
    status_id,
    age,
    salary,
    purchase_amount,
    transaction_count,
    effective_from,
    effective_to,
    current_flag,
    created_dt
FROM s_sql_dds.t_dm_task;