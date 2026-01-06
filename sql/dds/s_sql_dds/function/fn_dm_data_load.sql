DELIMITER //

CREATE PROCEDURE fn_dm_data_stg_to_dm_load(
    IN p_start_dt DATE,
    IN p_end_dt DATE
)
BEGIN
    DECLARE v_record_count INT;
    
    -- Подсчет записей для логирования
    SELECT COUNT(*) INTO v_record_count 
    FROM t_dm_stg_task
    WHERE (p_start_dt IS NULL OR effective_from >= p_start_dt)
      AND (p_end_dt IS NULL OR effective_to <= p_end_dt);
    
    -- Очистка данных за период в целевой таблице
    DELETE FROM t_dm_task 
    WHERE (p_start_dt IS NULL OR effective_from >= p_start_dt)
      AND (p_end_dt IS NULL OR effective_to <= p_end_dt);
    
    -- Вставка новых данных из staging таблицы
    INSERT INTO t_dm_task (
        fact_id, customer_id, product_id, region_id, status_id,
        age, salary, purchase_amount, transaction_count,
        effective_from, effective_to, current_flag, created_dt
    )
    SELECT 
        fact_id, customer_id, product_id, region_id, status_id,
        age, salary, purchase_amount, transaction_count,
        effective_from, effective_to, current_flag, created_dt
    FROM t_dm_stg_task
    WHERE (p_start_dt IS NULL OR effective_from >= p_start_dt)
      AND (p_end_dt IS NULL OR effective_to <= p_end_dt);
    
    -- Логирование результата
    SELECT CONCAT('Loaded ', ROW_COUNT(), ' records (total in staging: ', v_record_count, ')') AS result;
    
END //

DELIMITER ;