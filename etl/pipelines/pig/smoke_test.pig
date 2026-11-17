-- Load sample records from HDFS
data = LOAD '/user/nosql/pig-smoke/input'
    USING PigStorage(' ')
    AS (fruit:chararray, quantity:int);

-- Group records by fruit
grouped = GROUP data BY fruit;

-- Calculate total quantity for each fruit
totals = FOREACH grouped GENERATE
    group AS fruit,
    SUM(data.quantity) AS total_quantity;

-- Display the result
DUMP totals;
