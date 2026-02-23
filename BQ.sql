WITH 
-- Get the two most recent snapshot dates
latest_dates AS (
  SELECT DISTINCT SNAPSHOT_DT
  FROM ODS_ENGINE.SCHEMA
  ORDER BY SNAPSHOT_DT DESC
  LIMIT 2
),
today_dt AS (SELECT MAX(SNAPSHOT_DT) AS dt FROM latest_dates),
yesterday_dt AS (SELECT MIN(SNAPSHOT_DT) AS dt FROM latest_dates),

-- Today's and yesterday's snapshots
today AS (
  SELECT * FROM ODS_ENGINE.SCHEMA
  WHERE SNAPSHOT_DT = (SELECT dt FROM today_dt)
),
yesterday AS (
  SELECT * FROM ODS_ENGINE.SCHEMA
  WHERE SNAPSHOT_DT = (SELECT dt FROM yesterday_dt)
),

-- 1. NEW TABLES (entire table didn't exist yesterday)
new_tables AS (
  SELECT DISTINCT
    'NEW TABLE' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    NULL AS old_value,
    NULL AS new_value,
    'Table did not exist in previous snapshot' AS change_description
  FROM today t
  WHERE t.TABLE_NAME NOT IN (SELECT DISTINCT TABLE_NAME FROM yesterday)
),

-- 2. DROPPED TABLES (entire table missing from today)
dropped_tables AS (
  SELECT DISTINCT
    'DROPPED TABLE' AS change_type,
    y.TABLE_NAME,
    y.COLUMN_NAME,
    NULL AS old_value,
    NULL AS new_value,
    'Table no longer exists in current snapshot' AS change_description
  FROM yesterday y
  WHERE y.TABLE_NAME NOT IN (SELECT DISTINCT TABLE_NAME FROM today)
),

-- 3. NEW COLUMNS (column didn't exist in yesterday's snapshot for that table)
new_columns AS (
  SELECT
    'NEW COLUMN' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    NULL AS old_value,
    CONCAT('DATA_TYPE: ', IFNULL(t.DATA_TYPE,'NULL'),
           ' | IS_NULLABLE: ', IFNULL(t.IS_NULLABLE,'NULL'),
           ' | PRIMARY_KEY: ', IFNULL(t.PRIMARY_KEY,'NULL'),
           ' | FOREIGN_KEY: ', IFNULL(t.FOREIGN_KEY,'NULL')) AS new_value,
    'Column was added to the table' AS change_description
  FROM today t
  LEFT JOIN yesterday y
    ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE y.COLUMN_NAME IS NULL
    AND t.TABLE_NAME IN (SELECT DISTINCT TABLE_NAME FROM yesterday) -- exclude brand new tables (already captured above)
),

-- 4. DROPPED COLUMNS (column existed yesterday but missing today)
dropped_columns AS (
  SELECT
    'DROPPED COLUMN' AS change_type,
    y.TABLE_NAME,
    y.COLUMN_NAME,
    CONCAT('DATA_TYPE: ', IFNULL(y.DATA_TYPE,'NULL'),
           ' | IS_NULLABLE: ', IFNULL(y.IS_NULLABLE,'NULL'),
           ' | PRIMARY_KEY: ', IFNULL(y.PRIMARY_KEY,'NULL'),
           ' | FOREIGN_KEY: ', IFNULL(y.FOREIGN_KEY,'NULL')) AS old_value,
    NULL AS new_value,
    'Column was removed from the table' AS change_description
  FROM yesterday y
  LEFT JOIN today t
    ON y.TABLE_NAME = t.TABLE_NAME AND y.COLUMN_NAME = t.COLUMN_NAME
  WHERE t.COLUMN_NAME IS NULL
    AND y.TABLE_NAME IN (SELECT DISTINCT TABLE_NAME FROM today) -- exclude dropped tables (already captured above)
),

-- 5. DATA TYPE CHANGES
datatype_changes AS (
  SELECT
    'DATA TYPE CHANGE' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    y.DATA_TYPE AS old_value,
    t.DATA_TYPE AS new_value,
    CONCAT('Data type changed from [', IFNULL(y.DATA_TYPE,'NULL'), '] to [', IFNULL(t.DATA_TYPE,'NULL'), ']') AS change_description
  FROM today t
  JOIN yesterday y ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.DATA_TYPE,'') != IFNULL(y.DATA_TYPE,'')
),

-- 6. NULLABILITY CHANGES
nullable_changes AS (
  SELECT
    'NULLABILITY CHANGE' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    y.IS_NULLABLE AS old_value,
    t.IS_NULLABLE AS new_value,
    CONCAT('Nullability changed from [', IFNULL(y.IS_NULLABLE,'NULL'), '] to [', IFNULL(t.IS_NULLABLE,'NULL'), ']') AS change_description
  FROM today t
  JOIN yesterday y ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.IS_NULLABLE,'') != IFNULL(y.IS_NULLABLE,'')
),

-- 7. PRIMARY KEY CHANGES
pk_changes AS (
  SELECT
    'PRIMARY KEY CHANGE' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    y.PRIMARY_KEY AS old_value,
    t.PRIMARY_KEY AS new_value,
    CONCAT('Primary key flag changed from [', IFNULL(y.PRIMARY_KEY,'NULL'), '] to [', IFNULL(t.PRIMARY_KEY,'NULL'), ']') AS change_description
  FROM today t
  JOIN yesterday y ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.PRIMARY_KEY,'') != IFNULL(y.PRIMARY_KEY,'')
),

-- 8. FOREIGN KEY CHANGES
fk_changes AS (
  SELECT
    'FOREIGN KEY CHANGE' AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    y.FOREIGN_KEY AS old_value,
    t.FOREIGN_KEY AS new_value,
    CONCAT('Foreign key changed from [', IFNULL(y.FOREIGN_KEY,'NULL'), '] to [', IFNULL(t.FOREIGN_KEY,'NULL'), ']') AS change_description
  FROM today t
  JOIN yesterday y ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.FOREIGN_KEY,'') != IFNULL(y.FOREIGN_KEY,'')
),

-- 9. DESCRIPTION CHANGES (including DEPRECATED detection)
description_changes AS (
  SELECT
    CASE 
      WHEN LOWER(IFNULL(t.DESCRIPTION,'')) LIKE '%deprecated%' 
       AND LOWER(IFNULL(y.DESCRIPTION,'')) NOT LIKE '%deprecated%'
      THEN '⚠️ DEPRECATED COLUMN'
      ELSE 'DESCRIPTION CHANGE'
    END AS change_type,
    t.TABLE_NAME,
    t.COLUMN_NAME,
    y.DESCRIPTION AS old_value,
    t.DESCRIPTION AS new_value,
    CASE 
      WHEN LOWER(IFNULL(t.DESCRIPTION,'')) LIKE '%deprecated%' 
       AND LOWER(IFNULL(y.DESCRIPTION,'')) NOT LIKE '%deprecated%'
      THEN CONCAT('⚠️ COLUMN MARKED AS DEPRECATED. Description changed from [', IFNULL(y.DESCRIPTION,'NULL'), '] to [', IFNULL(t.DESCRIPTION,'NULL'), ']')
      ELSE CONCAT('Description changed from [', IFNULL(y.DESCRIPTION,'NULL'), '] to [', IFNULL(t.DESCRIPTION,'NULL'), ']')
    END AS change_description
  FROM today t
  JOIN yesterday y ON t.TABLE_NAME = y.TABLE_NAME AND t.COLUMN_NAME = y.COLUMN_NAME
  WHERE IFNULL(t.DESCRIPTION,'') != IFNULL(y.DESCRIPTION,'')
),

-- UNION ALL CHANGES
all_changes AS (
  SELECT * FROM new_tables
  UNION ALL SELECT * FROM dropped_tables
  UNION ALL SELECT * FROM new_columns
  UNION ALL SELECT * FROM dropped_columns
  UNION ALL SELECT * FROM datatype_changes
  UNION ALL SELECT * FROM nullable_changes
  UNION ALL SELECT * FROM pk_changes
  UNION ALL SELECT * FROM fk_changes
  UNION ALL SELECT * FROM description_changes
)

SELECT
  (SELECT dt FROM today_dt)      AS snapshot_dt_today,
  (SELECT dt FROM yesterday_dt)  AS snapshot_dt_yesterday,
  change_type,
  TABLE_NAME,
  COLUMN_NAME,
  old_value,
  new_value,
  change_description
FROM all_changes
ORDER BY
  -- Prioritise deprecated and structural changes at the top
  CASE change_type
    WHEN '⚠️ DEPRECATED COLUMN' THEN 1
    WHEN 'DROPPED TABLE'         THEN 2
    WHEN 'NEW TABLE'             THEN 3
    WHEN 'DROPPED COLUMN'        THEN 4
    WHEN 'NEW COLUMN'            THEN 5
    WHEN 'DATA TYPE CHANGE'      THEN 6
    WHEN 'PRIMARY KEY CHANGE'    THEN 7
    WHEN 'FOREIGN KEY CHANGE'    THEN 8
    WHEN 'NULLABILITY CHANGE'    THEN 9
    ELSE                              10
  END,
  TABLE_NAME,
  COLUMN_NAME;
