USE ${hivevar:db_name};

DROP TABLE IF EXISTS ${hivevar:parsed_table};
CREATE TABLE ${hivevar:parsed_table} STORED AS PARQUET AS
WITH ranked AS (
    SELECT
        raw_line,
        CAST(regexp_extract(INPUT__FILE__NAME, '/([0-9]{2})_', 1) AS INT) AS file_rank,
        row_number() OVER (
            PARTITION BY INPUT__FILE__NAME
            ORDER BY 1
        ) AS local_seq
    FROM ${hivevar:raw_table}
),
numbered AS (
    SELECT
        row_number() OVER (
            ORDER BY file_rank ASC, local_seq ASC
        ) AS line_number,
        raw_line
    FROM ranked
),
outer_parsed AS (
    SELECT
        line_number,
        CAST((line_number - 1) / ${hivevar:batch_size} + 1 AS INT) AS batch_id,
        raw_line,
        regexp_extract(
            raw_line,
            '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$',
            1
        ) AS host,
        regexp_extract(
            raw_line,
            '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$',
            4
        ) AS ts_raw,
        regexp_extract(
            raw_line,
            '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$',
            5
        ) AS req_raw,
        regexp_extract(
            raw_line,
            '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$',
            6
        ) AS status_raw,
        regexp_extract(
            raw_line,
            '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$',
            7
        ) AS bytes_raw
    FROM numbered
),
req_split AS (
    SELECT
        line_number,
        batch_id,
        (host IS NULL OR host = '' OR ts_raw IS NULL OR ts_raw = '' OR status_raw IS NULL OR status_raw = '') AS malformed,
        host,
        ts_raw,
        status_raw,
        bytes_raw,
        regexp_extract(req_raw, '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 3) AS proto_with_p,
        regexp_extract(req_raw, '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 1) AS method_with_p,
        regexp_extract(req_raw, '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 2) AS path_with_p,
        regexp_extract(req_raw, '(?s)^ *([^ ]+)(?: +(.*?))? *$', 1) AS method_no_p,
        regexp_extract(req_raw, '(?s)^ *([^ ]+)(?: +(.*?))? *$', 2) AS path_no_p
    FROM outer_parsed
),
normalized AS (
    SELECT
        line_number,
        batch_id,
        malformed,
        CASE WHEN malformed THEN CAST(NULL AS STRING) ELSE host END AS host,
        CASE
            WHEN malformed THEN CAST(NULL AS STRING)
            ELSE concat(
                substr(ts_raw, 8, 4), '-',
                CASE substr(ts_raw, 4, 3)
                    WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                    WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                    WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                END, '-',
                substr(ts_raw, 1, 2)
            )
        END AS log_date,
        CASE WHEN malformed THEN CAST(NULL AS INT) ELSE CAST(substr(ts_raw, 13, 2) AS INT) END AS log_hour,
        CASE
            WHEN malformed THEN CAST(NULL AS STRING)
            WHEN proto_with_p IS NOT NULL AND proto_with_p <> '' THEN
                CASE WHEN method_with_p = '' THEN CAST(NULL AS STRING) ELSE method_with_p END
            ELSE
                CASE WHEN method_no_p = '' THEN CAST(NULL AS STRING) ELSE method_no_p END
        END AS http_method,
        CASE
            WHEN malformed THEN CAST(NULL AS STRING)
            WHEN proto_with_p IS NOT NULL AND proto_with_p <> '' THEN
                CASE WHEN path_with_p IS NULL OR path_with_p = '' THEN CAST(NULL AS STRING) ELSE path_with_p END
            ELSE
                CASE WHEN path_no_p IS NULL OR path_no_p = '' THEN CAST(NULL AS STRING) ELSE path_no_p END
        END AS resource_path,
        CASE
            WHEN malformed THEN CAST(NULL AS STRING)
            WHEN proto_with_p IS NOT NULL AND proto_with_p <> '' THEN proto_with_p
            ELSE CAST(NULL AS STRING)
        END AS protocol_version,
        CASE WHEN malformed THEN CAST(NULL AS INT) ELSE CAST(status_raw AS INT) END AS status_code,
        CASE
            WHEN malformed THEN CAST(NULL AS BIGINT)
            WHEN bytes_raw = '-' THEN CAST(0 AS BIGINT)
            ELSE CAST(bytes_raw AS BIGINT)
        END AS bytes_transferred
    FROM req_split
)
SELECT * FROM normalized;
