SELECT
  t1.step1 AS step1,
  t2.step2 AS step2,
  t1.date AS dateF,
  t3.step3 AS step3,
  t4.step4 AS step4,
  t5.step5 AS step5,
  t1.geoNetwork.region AS Region,
  t1.device.deviceCategory AS Device,
  t6.Users AS Users,
  t1.hits.page.hostname AS Hostname,
  t1.trafficSource.medium as Medium,
  t1.trafficSource.source as Source
FROM (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    EXACT_COUNT_DISTINCT(CONCAT(CAST(fullVisitorID AS STRING),CAST(visitID AS STRING))) AS Step1,
    CONCAT(hits.eventInfo.eventAction,device.deviceCategory) AS ActDevCat
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.eventInfo.eventAction, '^Step$|^Step 1$'))
    AND (REGEXP_MATCH(hits.page.hostname,'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    ActDevCat,
    5,
    6
  HAVING
    NOT REGEXP_MATCH(ActDevCat, '^Step 1desktop$|^Stepmobile$')) t1
JOIN (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    EXACT_COUNT_DISTINCT(CONCAT(CAST(fullVisitorID AS STRING),CAST(visitID AS STRING))) AS Step2
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.eventInfo.eventAction, '^Step 3$'))
    AND (REGEXP_MATCH(hits.page.hostname, 'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)t2
ON
  t1.date = t2.date
  AND t1.geoNetwork.region = t2.geoNetwork.region
  AND t1.device.deviceCategory = t2.device.deviceCategory
  AND t1.hits.page.hostname = t2.hits.page.hostname
  AND t1.trafficSource.medium = t2.trafficSource.medium
  AND t1.trafficSource.source = t2.trafficSource.source
JOIN (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    COUNT(DISTINCT fullVisitorId) AS Step3
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.eventInfo.eventAction, '^Step 4$'))
    AND (REGEXP_MATCH(hits.page.hostname, 'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)t3
ON
  t1.date = t3.date
  AND t1.geoNetwork.region = t3.geoNetwork.region
  AND t1.device.deviceCategory = t3.device.deviceCategory
  AND t1.hits.page.hostname = t3.hits.page.hostname
  AND t1.trafficSource.medium = t3.trafficSource.medium
  AND t1.trafficSource.source = t3.trafficSource.source
JOIN (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    COUNT(DISTINCT fullVisitorId) AS Step4
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.eventInfo.eventAction, '^Step 5$'))
    AND (REGEXP_MATCH(hits.page.hostname, 'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)t4
ON
  t1.date = t4.date
  AND t1.geoNetwork.region = t4.geoNetwork.region
  AND t1.device.deviceCategory = t4.device.deviceCategory
  AND t1.hits.page.hostname = t4.hits.page.hostname
  AND t1.trafficSource.medium = t4.trafficSource.medium
  AND t1.trafficSource.source = t4.trafficSource.source
JOIN (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    COUNT(DISTINCT fullVisitorId) AS Step5
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.eventInfo.eventAction, '^Step 6$'))
    AND (REGEXP_MATCH(hits.page.hostname, 'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)t5
ON
  t1.date = t5.date
  AND t1.geoNetwork.region = t5.geoNetwork.region
  AND t1.device.deviceCategory = t5.device.deviceCategory
  AND t1.hits.page.hostname = t5.hits.page.hostname
  AND t1.trafficSource.medium = t5.trafficSource.medium
  AND t1.trafficSource.source = t5.trafficSource.source
JOIN (
  SELECT
    trafficSource.medium,
    trafficSource.source,
    DATE,
    geoNetwork.region,
    device.deviceCategory,
    hits.page.hostname,
    COUNT(DISTINCT fullVisitorId) AS Users
  FROM
    TABLE_DATE_RANGE_STRICT([64786069.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'YEAR')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, 'DAY')))
  WHERE
    (REGEXP_MATCH(hits.page.hostname, 'www.roomstogokids.com|m.roomstogokids.com|m.roomstogokids.com.googleweblight.com|roomstogokids.com|roomstogo.com|m.roomstogo.com.googleweblight.com|www.roomstogo.com|m.roomstogo.com'))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6) t6
ON
  t1.date = t6.date
  AND t1.geoNetwork.region = t6.geoNetwork.region
  AND t1.device.deviceCategory = t6.device.deviceCategory
  AND t1.hits.page.hostname = t6.hits.page.hostname
  AND t1.trafficSource.medium = t6.trafficSource.medium
  AND t1.trafficSource.source = t6.trafficSource.source
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12