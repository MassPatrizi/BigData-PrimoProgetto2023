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
    time INT,
    summary STRING,
    text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");


-- Caricamento dei dati nella tabella reviews
--LOAD DATA LOCAL INPATH 'BigData/Job1/Reviews.csv' INTO TABLE reviews;
LOAD DATA LOCAL INPATH 'Desktop/ReviewsFTD.csv' INTO TABLE reviews;


-- Generazione dei risultati per ciascun anno
-- reviews_by_year seleziona tutte le ennuple prendendo year, productid e text
WITH reviews_by_year AS (
    SELECT
        year(FROM_UNIXTIME(time)) AS year,
        productid,
        text
    FROM
        reviews
),
-- top_products conta tutte le righe con uno stesso year e productid
top_products AS (
    SELECT
        year,
        productid,
        COUNT(*) AS review_count    --quante volte appaiono insieme un year e productid
    FROM
        reviews_by_year
    GROUP BY
        year,
        productid
),
top_10_products AS (
    SELECT
        year,
        productid,
        review_count,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY review_count DESC) AS rank
    FROM
        top_products
),
tw AS (
    SELECT
        year,
        productid,
        word,
        word_count,
        ROW_NUMBER() OVER (PARTITION BY year, productid ORDER BY word_count DESC) AS rank
    FROM (
        SELECT
            year,
            productid,
            word,
            COUNT(*) AS word_count
        FROM
            reviews_by_year
        LATERAL VIEW EXPLODE(SPLIT(text, ' ')) word_table AS word
        WHERE
            LENGTH(word) >= 4
        GROUP BY
            year,
            productid,
            word
    ) word_counts
)
SELECT
    tp.year,
    tp.productid,
    tp.review_count,
    tw.word,
    tw.word_count
FROM
    top_10_products tp
JOIN 
    tw ON tp.year = tw.year AND tp.productid = tw.productid AND tw.rank <= 5
WHERE
    tp.rank <= 10
ORDER BY year ASC, review_count, productid, word_count DESC;