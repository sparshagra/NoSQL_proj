jul = LOAD '/user/nosql/linecount/input/NASA_access_log_Jul95'
    USING TextLoader()
    AS (line:chararray);

jul_group = GROUP jul ALL;
jul_count = FOREACH jul_group GENERATE
    'Jul95' AS file,
    COUNT(jul) AS line_count;

aug = LOAD '/user/nosql/linecount/input/NASA_access_log_Aug95'
    USING TextLoader()
    AS (line:chararray);

aug_group = GROUP aug ALL;
aug_count = FOREACH aug_group GENERATE
    'Aug95' AS file,
    COUNT(aug) AS line_count;

DUMP jul_count;
DUMP aug_count;
