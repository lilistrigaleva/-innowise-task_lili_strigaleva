USE ROLE ACCOUNTADMIN;
USE DATABASE AIRLINES_DWH;

CREATE OR REPLACE PROCEDURE CORE_DATA.LOAD_AIRLINES_TO_CORE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT DEFAULT 0;
BEGIN
    -- 1. Проверяем стрим
    IF (SYSTEM$STREAM_HAS_DATA('RAW_DATA.AIRLINES_STREAM')) THEN
        
        -- Начинаем явную транзакцию (хороший тон)
        BEGIN TRANSACTION;

        -- 2. Вставка данных
        INSERT INTO CORE_DATA.AIRLINES (
            PASSENGER_ID, FIRST_NAME, LAST_NAME, AGE, DEPARTURE_DATE, 
            FLIGHT_STATUS, AIRPORT_COUNTRY_CODE, COUNTRY_NAME
        )
        SELECT 
            TRIM(PASSENGER_ID),
            TRIM(FIRST_NAME),
            TRIM(LAST_NAME),
            TRY_TO_NUMBER(AGE),
            TRY_TO_DATE(DEPARTURE_DATE, 'MM/DD/YYYY'),
            TRIM(FLIGHT_STATUS),
            TRIM(AIRPORT_COUNTRY_CODE),
            TRIM(COUNTRY_NAME)
        FROM RAW_DATA.AIRLINES_STREAM
        WHERE METADATA$ACTION = 'INSERT' 
        AND METADATA$ISUPDATE = FALSE;

        rows_inserted := SQLROWCOUNT;

        -- 3. Логирование
        INSERT INTO METADATA.AUDIT_LOG (PROCESS_NAME, STATUS, ROWS_AFFECTED)
        VALUES ('LOAD_AIRLINES_TO_CORE', 'SUCCESS', :rows_inserted);

        -- !!! ВАЖНО: Явно сохраняем изменения !!!
        COMMIT;

        RETURN 'Success: ' || :rows_inserted || ' rows loaded.';
        
    ELSE
        -- Стрим пуст
        INSERT INTO METADATA.AUDIT_LOG (PROCESS_NAME, STATUS, ROWS_AFFECTED)
        VALUES ('LOAD_AIRLINES_TO_CORE', 'SKIPPED', 0);
        
        -- Тут тоже закоммитим лог на всякий случай
        COMMIT;
        
        RETURN 'Skipped: No new data found.';
    END IF;

EXCEPTION
    WHEN OTHER THEN
        -- Если ошибка - отменяем изменения в данных
        ROLLBACK;
        
        -- И записываем ошибку в лог (в новой транзакции)
        INSERT INTO METADATA.AUDIT_LOG (PROCESS_NAME, STATUS, ERROR_MESSAGE)
        VALUES ('LOAD_AIRLINES_TO_CORE', 'ERROR', :SQLERRM);
        COMMIT;
        
        RETURN 'Error: ' || SQLERRM;
END;
$$;