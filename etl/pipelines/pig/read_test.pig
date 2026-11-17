data = LOAD '/user/nosql/linecount/input/NASA_access_log_Jul95'
    USING TextLoader()
    AS (line:chararray);

first_three = LIMIT data 3;

DUMP first_three;
