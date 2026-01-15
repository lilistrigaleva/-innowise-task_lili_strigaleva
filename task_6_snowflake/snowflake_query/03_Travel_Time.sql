USE ROLE ACCOUNTADMIN;
USE DATABASE AIRLINES_DWH;
USE SCHEMA CORE_DATA;

-- === 1. DML Просмотр данных, какими они были 3 минуты назад 
SELECT * FROM AIRLINES 
AT(OFFSET => -60*3) 
LIMIT 10;

-- === 2. DML Просмотр данных на конкретный момент времени 
SELECT * FROM AIRLINES 
AT(TIMESTAMP => DATEADD(minute, -10, CURRENT_TIMESTAMP()))
LIMIT 10;

-- === 3. DDL Создание копии таблицы на основе её состояния 2 минуты назад
CREATE OR REPLACE TABLE AIRLINES_SNAPSHOT 
CLONE AIRLINES AT(OFFSET => -60*2);

-- === 4. DDL Демонстрация восстановления случайно удаленной таблицы
DROP TABLE AIRLINES_SNAPSHOT;
UNDROP TABLE AIRLINES_SNAPSHOT;