USE ${hivevar:db_name};

INSERT OVERWRITE DIRECTORY '${hivevar:out_meta_dir}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
SELECT
    COUNT(*),
    SUM(CASE WHEN malformed THEN 1 ELSE 0 END)
FROM ${hivevar:parsed_table};

INSERT OVERWRITE DIRECTORY '${hivevar:out_q1_dir}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
SELECT
    log_date,
    status_code,
    COUNT(*),
    SUM(bytes_transferred)
FROM ${hivevar:parsed_table}
WHERE NOT malformed
GROUP BY log_date, status_code;

INSERT OVERWRITE DIRECTORY '${hivevar:out_q2_dir}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
SELECT
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count
FROM (
    SELECT
        resource_path,
        COUNT(*) AS request_count,
        SUM(bytes_transferred) AS total_bytes,
        COUNT(DISTINCT host) AS distinct_host_count
    FROM ${hivevar:parsed_table}
    WHERE NOT malformed
      AND resource_path IS NOT NULL
      AND resource_path <> ''
    GROUP BY resource_path
) q2_agg
ORDER BY request_count DESC, resource_path ASC
LIMIT ${hivevar:top_n};

INSERT OVERWRITE DIRECTORY '${hivevar:out_q3_dir}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*) AS total_request_count,
    CAST(SUM(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host END) AS distinct_error_hosts
FROM ${hivevar:parsed_table}
WHERE NOT malformed
GROUP BY log_date, log_hour;
