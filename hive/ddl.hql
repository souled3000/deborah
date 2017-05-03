CREATE TABLE IF NOT EXISTS his_dev (id BIGINT,mac STRING,dv INT,lt INT,ts INT) PARTITIONED BY (dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

SELECT dv,n,'2010090909' dt
FROM
(SELECT dv,COUNT(DISTINCT(id)) n FROM his_dev WHERE dt = '2010090909' AND lt = 2 GROUP BY dv
UNION
SELECT 0 dv,COUNT(DISTINCT(id)) n FROM his_dev WHERE dt = '2010090909' AND lt =2 )t ORDER BY dv

CREATE TABLE `his_dev` (
  `dv` int(10),
  `amount` bigint(10),
  `dt` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `his_dev_day` (
  `dv` int(10),
  `amount` bigint(10),
  `dt` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `his_dev_month` (
  `dv` int(10),
  `amount` bigint(10),
  `dt` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `his_dev_year` (
  `dv` int(10),
  `amount` bigint(10),
  `dt` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

