SELECT
  Device_Path,
  EXACT_COUNT_DISTINCT(User_ID) AS Users,
  SUM(Pageviews_User) AS Pageviews,
  SUM(Pageviews_User)/EXACT_COUNT_DISTINCT(User_ID) AS Pageviews_Per_User,
  SUM(Duration_User)/EXACT_COUNT_DISTINCT(User_ID) AS Time_Per_User,
  SUM(Transactions_User) AS Transactions,
  SUM(Transactions_User)/EXACT_COUNT_DISTINCT(User_ID) AS User_Conversion_Rate,
  SUM(Revenue_User) AS Revenue,
  SUM(Revenue_User)/EXACT_COUNT_DISTINCT(User_ID) AS Revenue_Per_User
FROM (
  SELECT
    User_ID,
    LAST_VALUE(Device_Paths) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Device_Path,
    SUM(Session) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Sessions_User,
    SUM(New) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS New_User,
    SUM(Pageview) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Pageviews_User,
    SUM(Duration) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Duration_User,
    SUM(Transaction) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Transactions_User,
    SUM(Transaction_Revenue) OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Revenue_User
  FROM (
    SELECT
      User_ID,
      GROUP_CONCAT(Path_Element, ' > ') OVER (PARTITION BY User_ID ORDER BY Session_ID) AS Device_Paths,
      Session,
      Pageview,
      Duration
    FROM (
      SELECT
        User_ID,
        Session_ID,
        CONCAT(Device_Category, ' (', Channel, ')') AS Path_Element,
        Session,
        New,
        Pageview,
        Duration,
        Transaction,
        Transaction_Revenue
      FROM (
        SELECT
          User_ID,
          Session_ID,
          Device_Category,
          CASE
            WHEN Medium=='cpc' THEN CONCAT('Paid Search', ' / ', IF(Source IS NOT NULL,Source,'(not set)'), ' / ', IF(Campaign IS NOT NULL,Campaign,'(not set)'))
            WHEN Medium=='organic' THEN 'Organic'
            WHEN Medium=='cse' THEN CONCAT('CSE', ' / ', IF(Source IS NOT NULL,Source,'(not set)'))
            WHEN Medium=='localsearch' THEN 'Local Search'
            WHEN Medium=='(none)' THEN 'Direct'
            WHEN (Medium=='social'
            AND Source=='facebook') THEN CONCAT('Facebook', ' / ', IF(Campaign IS NOT NULL,Campaign,'(not set)'))
            WHEN (Medium=='social' AND Source=='pinterest') THEN 'Pinterest'
            WHEN Medium=='cpm' THEN CONCAT('Display', ' / ', IF(Source IS NOT NULL,Source,'(not set)'), ' / ', IF(REGEXP_MATCH(Campaign,r'^_dfa_\d+:\d+:(\d+)$'), REGEXP_EXTRACT(Campaign,r'^_dfa_\d+:\d+:(\d+)$'), IF(Campaign IS NOT NULL,Campaign,'(not set)')))
            WHEN Medium=='referral' THEN 'Referral'
            WHEN Medium=='coupons' THEN 'Coupons'
            WHEN Medium=='email' THEN CONCAT('Email', ' / ', IF(Source IS NOT NULL,Source,'(not set)'), ' / ', IF(Campaign IS NOT NULL,Campaign,'(not set)'))
            ELSE 'Other'
          END AS Channel,
          Session,
          New,
          Pageview,
          Duration,
          Transaction,
          Transaction_Revenue
        FROM
          FLATTEN((
            SELECT
              visitId AS Session_ID,
              device.deviceCategory AS Device_Category,
              customDimensions.index AS CD_Index,
              customDimensions.value AS User_ID,
              trafficSource.source AS Source,
              trafficSource.medium AS Medium,
              trafficSource.campaign AS Campaign,
              totals.visits AS Session,
              totals.newVisits AS New,
              totals.pageviews AS Pageview,
              totals.timeOnSite AS Duration,
              totals.transactions AS Transaction,
              totals.totalTransactionRevenue/1000000 AS Transaction_Revenue
            FROM
              TABLE_DATE_RANGE( [64786069.ga_sessions_], TIMESTAMP('2016-01-01'), TIMESTAMP('2016-01-30'))
            HAVING
              CD_Index==31
              AND User_ID IS NOT NULL), User_ID)))))
GROUP BY
  Device_Path
ORDER BY
  Users DESC