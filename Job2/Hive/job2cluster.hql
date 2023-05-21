-- Creazione della tabella reviews
DROP TABLE reviews;

CREATE TABLE IF NOT EXISTS reviews (
    id STRING,
    productid STRING,
    userid STRING,
    profilename STRING,
    helpfulnessnumerator INT,
    helpfulnessdenominator INT,
    score INT,
    `time` INT,
    summary STRING,
    text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

-- Caricamento dei dati nella tabella reviews
LOAD DATA LOCAL INPATH 'decupled_ReviewsCleaned.csv' INTO TABLE reviews;

SELECT userid, SUM(helpfulnessnumerator)/SUM(helpfulnessdenominator) AS mean
FROM reviews
GROUP BY userid
ORDER BY mean DESC
LIMIT 10