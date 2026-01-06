CREATE OR REPLACE FUNCTION s_sql_dds.fn_dq_checks_load(
    start_dt DATE DEFAULT NULL,
    end_dt DATE DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_check_count INTEGER := 0;
    v_passed_count INTEGER := 0;
    v_failed_count INTEGER := 0;
    v_error_message VARCHAR;
    v_expected NUMERIC;
    v_actual NUMERIC;
BEGIN
    -- Очищаем предыдущие результаты за период
    DELETE FROM s_sql_dds.t_dq_check_results 
    WHERE execution_date::DATE >= COALESCE(start_dt, '1900-01-01'::DATE)
      AND execution_date::DATE <= COALESCE(end_dt, '2100-12-31'::DATE);
    
    BEGIN
        v_check_count := v_check_count + 1;
        
        -- Ожидаемая сумма из источника
        SELECT COALESCE(SUM(purchase_amount), 0) INTO v_expected
        FROM s_sql_dds.t_sql_source_structured
        WHERE (start_dt IS NULL OR effective_from >= start_dt)
          AND (end_dt IS NULL OR effective_to <= end_dt);
        
        -- Фактическая сумма из витрины
        SELECT COALESCE(SUM(purchase_amount), 0) INTO v_actual
        FROM s_sql_dds.v_dm_task
        WHERE (start_dt IS NULL OR effective_from >= start_dt)
          AND (end_dt IS NULL OR effective_to <= end_dt);
        
        -- Проверяем разницу (допустима 1% погрешность)
        IF ABS(v_expected - v_actual) / NULLIF(v_expected, 0) <= 0.01 THEN
            v_passed_count := v_passed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, expected_value, actual_value, error_threshold, error_message)
            VALUES ('correctness', 'v_dm_task', 'Purchase amount sum comparison', 'passed', 
                    v_expected, v_actual, 0.01, 'Sum difference within acceptable range');
        ELSE
            v_failed_count := v_failed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, expected_value, actual_value, error_threshold, error_message)
            VALUES ('correctness', 'v_dm_task', 'Purchase amount sum comparison', 'failed', 
                    v_expected, v_actual, 0.01, 'Sum difference exceeds threshold');
        END IF;
    EXCEPTION WHEN OTHERS THEN
        v_failed_count := v_failed_count + 1;
        INSERT INTO s_sql_dds.t_dq_check_results 
        (check_type, table_name, check_name, status, error_message)
        VALUES ('correctness', 'v_dm_task', 'Purchase amount sum comparison', 'error', 
                'Error: ' || SQLERRM);
    END;
    
    BEGIN
        v_check_count := v_check_count + 1;
        
        -- Проверяем пропуски в customer_id
        SELECT 
            COUNT(*) FILTER (WHERE customer_id IS NULL) * 100.0 / NULLIF(COUNT(*), 0)
        INTO v_actual
        FROM s_sql_dds.v_dm_task
        WHERE (start_dt IS NULL OR effective_from >= start_dt)
          AND (end_dt IS NULL OR effective_to <= end_dt);
        
        -- Допустимо до 5% пропусков
        IF COALESCE(v_actual, 0) <= 5 THEN
            v_passed_count := v_passed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('completeness', 'v_dm_task', 'customer_id', 'Null values percentage', 'passed', 
                    v_actual, 5, 'Null values within acceptable range');
        ELSE
            v_failed_count := v_failed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('completeness', 'v_dm_task', 'customer_id', 'Null values percentage', 'failed', 
                    v_actual, 5, 'Too many null values');
        END IF;
    EXCEPTION WHEN OTHERS THEN
        v_failed_count := v_failed_count + 1;
        INSERT INTO s_sql_dds.t_dq_check_results 
        (check_type, table_name, check_name, status, error_message)
        VALUES ('completeness', 'v_dm_task', 'Null values check', 'error', 
                'Error: ' || SQLERRM);
    END;
    
    BEGIN
        v_check_count := v_check_count + 1;
        
        -- Проверяем некорректные даты
        SELECT COUNT(*) INTO v_actual
        FROM s_sql_dds.v_dm_task
        WHERE effective_to < effective_from
          AND (start_dt IS NULL OR effective_from >= start_dt)
          AND (end_dt IS NULL OR effective_to <= end_dt);
        
        -- Должно быть 0 некорректных записей
        IF v_actual = 0 THEN
            v_passed_count := v_passed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('consistency', 'v_dm_task', 'Date range validation', 'passed', 
                    v_actual, 0, 'All date ranges are valid');
        ELSE
            v_failed_count := v_failed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('consistency', 'v_dm_task', 'Date range validation', 'failed', 
                    v_actual, 0, 'Found invalid date ranges');
        END IF;
    EXCEPTION WHEN OTHERS THEN
        v_failed_count := v_failed_count + 1;
        INSERT INTO s_sql_dds.t_dq_check_results 
        (check_type, table_name, check_name, status, error_message)
        VALUES ('consistency', 'v_dm_task', 'Date range validation', 'error', 
                'Error: ' || SQLERRM);
    END;
    
    BEGIN
        v_check_count := v_check_count + 1;
        
        -- Проверяем дубликаты по ключевым полям
        WITH duplicates AS (
            SELECT fact_id, customer_id, effective_from,
                   COUNT(*) as duplicate_count
            FROM s_sql_dds.v_dm_task
            WHERE (start_dt IS NULL OR effective_from >= start_dt)
              AND (end_dt IS NULL OR effective_to <= end_dt)
            GROUP BY fact_id, customer_id, effective_from
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) INTO v_actual FROM duplicates;
        
        -- Должно быть 0 дубликатов
        IF v_actual = 0 THEN
            v_passed_count := v_passed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('uniqueness', 'v_dm_task', 'Duplicate records check', 'passed', 
                    v_actual, 0, 'No duplicate records found');
        ELSE
            v_failed_count := v_failed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('uniqueness', 'v_dm_task', 'Duplicate records check', 'failed', 
                    v_actual, 0, 'Found duplicate records');
        END IF;
    EXCEPTION WHEN OTHERS THEN
        v_failed_count := v_failed_count + 1;
        INSERT INTO s_sql_dds.t_dq_check_results 
        (check_type, table_name, check_name, status, error_message)
        VALUES ('uniqueness', 'v_dm_task', 'Duplicate check', 'error', 
                'Error: ' || SQLERRM);
    END;
    
    BEGIN
        v_check_count := v_check_count + 1;
        
        -- Проверяем зарплату в допустимом диапазоне (0 - 1,000,000)
        SELECT COUNT(*) INTO v_actual
        FROM s_sql_dds.v_dm_task
        WHERE (salary < 0 OR salary > 1000000)
          AND (start_dt IS NULL OR effective_from >= start_dt)
          AND (end_dt IS NULL OR effective_to <= end_dt);
        
        -- Должно быть 0 записей с невалидной зарплатой
        IF v_actual = 0 THEN
            v_passed_count := v_passed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('validity', 'v_dm_task', 'salary', 'Salary range validation', 'passed', 
                    v_actual, 0, 'All salary values are valid');
        ELSE
            v_failed_count := v_failed_count + 1;
            INSERT INTO s_sql_dds.t_dq_check_results 
            (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
            VALUES ('validity', 'v_dm_task', 'salary', 'Salary range validation', 'failed', 
                    v_actual, 0, 'Found invalid salary values');
        END IF;
    EXCEPTION WHEN OTHERS THEN
        v_failed_count := v_failed_count + 1;
        INSERT INTO s_sql_dds.t_dq_check_results 
        (check_type, table_name, check_name, status, error_message)
        VALUES ('validity', 'v_dm_task', 'Salary validation', 'error', 
                'Error: ' || SQLERRM);
    END;
    
    INSERT INTO s_sql_dds.t_dq_check_results 
    (check_type, table_name, check_name, status, expected_value, actual_value, error_message)
    VALUES ('summary', 'v_dm_task', 'Overall DQ check', 
            CASE WHEN v_failed_count = 0 THEN 'passed' ELSE 'failed' END,
            v_check_count, v_passed_count,
            CONCAT('Total: ', v_check_count, ', Passed: ', v_passed_count, ', Failed: ', v_failed_count));
    
END;
$$ LANGUAGE plpgsql;