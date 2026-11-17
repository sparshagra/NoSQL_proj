-- Parse the outer Common Log Format record.
outer_parsed = FOREACH all_lines GENERATE
    line_number,
    line,
    REGEX_EXTRACT(line,
      '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$', 1) AS host,
    REGEX_EXTRACT(line,
      '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$', 4) AS timestamp,
    REGEX_EXTRACT(line,
      '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$', 5) AS request,
    REGEX_EXTRACT(line,
      '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$', 6) AS status_text,
    REGEX_EXTRACT(line,
      '(?s)^([^ ]+) ([^ ]+) ([^ ]+) \\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\\] "(.*)" ([0-9]{3}) ([0-9]+|-)$', 7) AS bytes_text;

-- Split requests using the reference parser's two-stage rule.
request_parts = FOREACH outer_parsed GENERATE
    *,
    REGEX_EXTRACT(request,
      '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 1) AS proto_method,
    REGEX_EXTRACT(request,
      '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 2) AS proto_path,
    REGEX_EXTRACT(request,
      '(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$', 3) AS protocol,
    REGEX_EXTRACT(request,
      '(?s)^ *([^ ]+)(?: +(.*?))? *$', 1) AS fallback_method,
    REGEX_EXTRACT(request,
      '(?s)^ *([^ ]+)(?: +(.*?))? *$', 2) AS fallback_path;

parsed = FOREACH request_parts GENERATE
    line_number,
    host,
    timestamp,
    status_text,
    bytes_text,
    (protocol IS NOT NULL ? proto_method : fallback_method) AS method,
    (protocol IS NOT NULL ? proto_path : fallback_path) AS resource_path,
    protocol;

-- Keep valid records for the three analytics queries.
valid_base = FILTER parsed BY
    host IS NOT NULL AND timestamp IS NOT NULL AND status_text IS NOT NULL;

with_month = FOREACH valid_base GENERATE
    line_number,
    host,
    resource_path,
    (int)status_text AS status_code,
    (bytes_text == '-' ? 0L : (long)bytes_text) AS total_bytes,
    timestamp,
    INDEXOF('JanFebMarAprMayJunJulAugSepOctNovDec',
            SUBSTRING(timestamp, 3, 6), 0) AS month_index;

dated = FOREACH with_month GENERATE
    line_number,
    host,
    resource_path,
    status_code,
    total_bytes,
    CONCAT(
      CONCAT(SUBSTRING(timestamp, 7, 11), '-'),
      CONCAT(
        SUBSTRING('010203040506070809101112',
                  ((int)(month_index / 3)) * 2,
                  ((int)(month_index / 3)) * 2 + 2),
        CONCAT('-', SUBSTRING(timestamp, 0, 2))
      )
    ) AS log_date,
    (int)SUBSTRING(timestamp, 12, 14) AS log_hour;

-- Q1: daily status counts and byte totals.
q1_group = GROUP dated BY (log_date, status_code);
q1 = FOREACH q1_group GENERATE
    group.$0 AS log_date,
    group.$1 AS status_code,
    COUNT_STAR(dated) AS request_count,
    SUM(dated.total_bytes) AS total_bytes;

-- Q2: exact per-resource distinct hosts, followed by global top 20.
q2_input = FILTER dated BY resource_path IS NOT NULL AND resource_path != '';
q2_group = GROUP q2_input BY resource_path;
q2_aggregate = FOREACH q2_group {
    unique_hosts = DISTINCT q2_input.host;
    GENERATE
        group AS resource_path,
        COUNT_STAR(q2_input) AS request_count,
        SUM(q2_input.total_bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_host_count;
};
q2_sorted = ORDER q2_aggregate BY request_count DESC, resource_path ASC PARALLEL 1;
q2 = LIMIT q2_sorted 20;

-- Q3: hourly totals and errors, including hours with zero errors.
q3_group = GROUP dated BY (log_date, log_hour);
q3 = FOREACH q3_group {
    error_rows = FILTER dated BY status_code >= 400 AND status_code <= 599;
    error_hosts = DISTINCT error_rows.host;
    GENERATE
        group.$0 AS log_date,
        group.$1 AS log_hour,
        COUNT_STAR(error_rows) AS error_request_count,
        COUNT_STAR(dated) AS total_request_count,
        (double)COUNT_STAR(error_rows) / (double)COUNT_STAR(dated) AS error_rate,
        COUNT(error_hosts) AS distinct_error_hosts;
};

STORE q1 INTO '$OUT_Q1' USING PigStorage('\u0001');
STORE q2 INTO '$OUT_Q2' USING PigStorage('\u0001');
STORE q3 INTO '$OUT_Q3' USING PigStorage('\u0001');
