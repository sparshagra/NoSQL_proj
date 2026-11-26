CREATE DATABASE IF NOT EXISTS ${hivevar:db_name};
USE ${hivevar:db_name};

DROP TABLE IF EXISTS ${hivevar:raw_table};
CREATE EXTERNAL TABLE ${hivevar:raw_table} (
    raw_line STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\036'
STORED AS TEXTFILE
LOCATION '${hivevar:hdfs_input_dir}';
