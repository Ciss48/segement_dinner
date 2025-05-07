-- new user by day, version, platform 
CREATE OR REPLACE TABLE `royal-hexa-in-house.dung_dinner_dashboard.new_user`
PARTITION BY event_date
CLUSTER BY  country,version AS
select 
  event_date,
  country,
  version,
  count(distinct user_pseudo_id) as new_user
from `royal-hexa-in-house.dung_dinner_flatten.first_open` 
group by  event_date,country, version;

-- dau
CREATE OR REPLACE TABLE `royal-hexa-in-house.dung_dinner_dashboard.dau`
PARTITION BY event_date
CLUSTER BY  country,version AS
select 
  event_date,
  country,
  version,
  count(distinct fo.user_pseudo_id) as dau
from `royal-hexa-in-house.dung_dinner_flatten.session_start` fo 
group by  event_date,country, version;

-- playtime
CREATE OR REPLACE TABLE `royal-hexa-in-house.dung_dinner_dashboard.playtime`
PARTITION BY event_date
CLUSTER BY  country,version AS
WITH screen_view_agg AS (
  SELECT 
   version,
    event_date,
    country,
    SUM(engagement_time_msec) as screen_view_time
  FROM `royal-hexa-in-house.dung_dinner_flatten.screen_view`
  where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
  GROUP BY version, event_date, country
),
user_engagement_agg AS (
  SELECT 
    version,
    event_date,
    country,
    COUNT(DISTINCT user_pseudo_id) as num_user,
    COUNT(DISTINCT ga_session_id) as total_session,
    SUM(engagement_time_msec) as engagement_time
  FROM `royal-hexa-in-house.dung_dinner_flatten.user_engagement`
  where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
  GROUP BY version, event_date, country
)
SELECT 
  ue.version as version,
  ue.event_date as event_date,
  ue.country as country,
  ue.num_user,
  ue.total_session,
  ROUND((COALESCE(ue.engagement_time, 0) + COALESCE(sv.screen_view_time, 0)) / 600000, 2) as total_session_length_minute
FROM user_engagement_agg ue 
FULL OUTER JOIN screen_view_agg sv
  ON ue.version = sv.version
  AND ue.event_date = sv.event_date
  AND ue.country = sv.country;



-- Cohort
CREATE OR REPLACE TABLE `royal-hexa-in-house.dung_dinner_dashboard.cohort`
PARTITION BY event_date
CLUSTER BY version AS
WITH first_open_users AS (
  SELECT DISTINCT user_pseudo_id, event_date AS cohort_date, version
  FROM `royal-hexa-in-house.dung_dinner_flatten.first_open`
),
session_users AS (
  SELECT DISTINCT user_pseudo_id, event_date, version
  FROM `royal-hexa-in-house.dung_dinner_flatten.session_start`
),
day0_users AS (
  SELECT cohort_date, version, COUNT(DISTINCT user_pseudo_id) as day0_users
  FROM first_open_users
  GROUP BY cohort_date, version
)
SELECT
  f.cohort_date as event_date,
  f.version,
  DATE_DIFF(s.event_date, f.cohort_date, DAY) AS days_since_first_open,
  COUNT(DISTINCT s.user_pseudo_id) AS retained_user,
  ROUND(COUNT(DISTINCT s.user_pseudo_id) / d.day0_users, 4) AS retention_rate
FROM first_open_users f
JOIN session_users s
  ON f.user_pseudo_id = s.user_pseudo_id
  AND s.event_date = DATE_ADD(f.cohort_date, INTERVAL DATE_DIFF(s.event_date, f.cohort_date, DAY) DAY)
JOIN day0_users d
  ON f.cohort_date = d.cohort_date
  AND f.version = d.version
GROUP BY f.cohort_date, f.version, d.day0_users, s.event_date; 


-- rev_iap_ads
CREATE OR REPLACE TABLE `royal-hexa-in-house.dung_dinner_dashboard.rev_iap_ads`
PARTITION BY event_date
CLUSTER BY version, country AS
WITH daily_metrics AS (
  SELECT 
    e.event_date,
    e.version,
    e.country,
    COUNT(DISTINCT e.user_pseudo_id) as total_user,
    COUNT(DISTINCT CASE WHEN i.event_value_in_usd > 0 THEN i.user_pseudo_id END) as user_pay,
    COALESCE(SUM(a.value), 0) as ads_rev,
    (COALESCE(SUM(i.event_value_in_usd), 0)) as iap_rev
  FROM (
    SELECT event_date, version, country, COUNT(DISTINCT user_pseudo_id) AS total_user, user_pseudo_id
    FROM `royal-hexa-in-house.dung_dinner_flatten.user_engagement`
    where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
    GROUP BY event_date, version, country, user_pseudo_id
    ) e
  LEFT JOIN (
    SELECT user_pseudo_id, version, country, event_date, SUM(value) AS value, ad_format
    FROM `royal-hexa-in-house.dung_dinner_flatten.ad_impression`
    where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
    GROUP BY user_pseudo_id, version, country, event_date, ad_format
    ) a
    ON e.user_pseudo_id = a.user_pseudo_id
        AND e.event_date = a.event_date
        AND e.version = a.version
        AND e.country = a.country
  LEFT JOIN (
    SELECT user_pseudo_id,  version, country, event_date, SUM(event_value_in_usd) as event_value_in_usd
    FROM `royal-hexa-in-house.dung_dinner_flatten.in_app_purchase`
    where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
    GROUP BY user_pseudo_id, version, country, event_date) i
    ON e.user_pseudo_id = i.user_pseudo_id
        AND e.event_date = i.event_date
        AND e.version = i.version
        AND e.country = i.country
  GROUP BY e.event_date, e.version, e.country
)
SELECT 
  event_date,
  version,
  country,
  iap_rev,
  ads_rev,
  (iap_rev + ads_rev) as total_revenue,
  total_user,
  user_pay
FROM daily_metrics;



-- ads_inter_rw_cnt
CREATE or replace TABLE `royal-hexa-in-house.dung_dinner_dashboard.ads_inter_rw_cnt`
PARTITION BY event_date
CLUSTER BY country, version AS 
WITH user_engagement_agg AS (
  SELECT 
    event_date, version,
    country,
    COUNT(DISTINCT user_pseudo_id) as total_user
  FROM `royal-hexa-in-house.dung_dinner_flatten.user_engagement`
  where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
  GROUP BY event_date, version, country
),
ad_impression_agg AS (
  SELECT
    event_date, version, 
    country,
    COUNT(DISTINCT CASE WHEN ad_format = 'REWARDED' THEN user_pseudo_id END) as ad_rw_count,
    COUNT(DISTINCT CASE WHEN ad_format = 'INTER' THEN user_pseudo_id END) as ad_inter_count
  FROM `royal-hexa-in-house.dung_dinner_flatten.ad_impression`
  where user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.dung_dinner_flatten.first_open`)
  GROUP BY event_date, version, country
)
SELECT
  e.event_date,
  e.version,
  e.country,
  e.total_user,
  COALESCE(a.ad_rw_count, 0) as ad_rw_count,
  COALESCE(a.ad_inter_count, 0) as ad_inter_count
FROM user_engagement_agg e
LEFT JOIN ad_impression_agg a
  ON e.event_date = a.event_date
  AND e.version = a.version
  AND e.country = a.country;


-- arpu cohort
CREATE or replace TABLE `royal-hexa-in-house.dung_dinner_dashboard.arpu_cohort`
PARTITION BY event_date
CLUSTER BY country, version AS 
WITH cohort AS (
  SELECT
    user_pseudo_id AS user_id,
    event_date AS cohort_date, 
    version, 
    country, 
  FROM `royal-hexa-in-house.dung_dinner_flatten.first_open`
),
revenue AS (
SELECT
  hihi.revenue_date,
  hihi.user_id,
  hihi.version,
  hihi.country,
  IFNULL(hihi.rev_ads, 0) + IFNULL(hihi.rev_iap, 0) as revenue
FROM (
  SELECT
    event_date as revenue_date,
    user_pseudo_id as user_id,
    version,
    country,    
    SUM(IFNULL(event_value_in_usd, 0)) as rev_iap,
    0 as rev_ads
  FROM `royal-hexa-in-house.dung_dinner_flatten.in_app_purchase`
  GROUP BY 1,2,3,4
  UNION ALL
  SELECT 
    event_date as revenue_date,
    user_pseudo_id as user_id,
    version,
    country,
    0 as rev_iap,
    SUM(IFNULL(value, 0)) AS rev_ads
  FROM `royal-hexa-in-house.dung_dinner_flatten.ad_impression`
  GROUP BY 1,2,3,4
) as hihi
),
cohort_revenue AS (
  SELECT
    c.user_id,
    c.cohort_date,
    r.revenue_date,
    c.version,
    c.country,
    DATE_DIFF(r.revenue_date, c.cohort_date, DAY) AS day_number,
    IFNULL(r.revenue, 0) AS revenue
  FROM cohort c
  LEFT JOIN revenue r
    ON c.user_id = r.user_id
    AND c.version = r.version
    AND c.country = r.country
    AND r.revenue_date >= c.cohort_date
)
SELECT
  c.cohort_date as event_date,
  c.version,
  c.country,
  COUNT(DISTINCT c.user_id) AS total_users,
  COALESCE(SUM(CASE WHEN cr.day_number = 0 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS Day_0_ARPU,
  COALESCE(SUM(CASE WHEN cr.day_number <= 1 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_1,
  COALESCE(SUM(CASE WHEN cr.day_number <= 2 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_2,
  COALESCE(SUM(CASE WHEN cr.day_number <= 3 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_3,
  COALESCE(SUM(CASE WHEN cr.day_number <= 4 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_4,
  COALESCE(SUM(CASE WHEN cr.day_number <= 5 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_5,
  COALESCE(SUM(CASE WHEN cr.day_number <= 6 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_6,
  COALESCE(SUM(CASE WHEN cr.day_number <= 7 THEN cr.revenue END), 0) / COUNT(DISTINCT c.user_id) AS LTV_Day_7,
  COALESCE(SUM(cr.revenue), 0) / COUNT(DISTINCT c.user_id) AS LTV_Total
FROM cohort c
LEFT JOIN cohort_revenue cr
  ON c.user_id = cr.user_id and c.version = cr.version and c.country = cr.country
GROUP BY 1,2,3;



-- drop 
CREATE or replace TABLE `royal-hexa-in-house.dung_dinner_dashboard.drop`
CLUSTER BY level, version AS 
with level_stats AS (
  SELECT 
    version,
    cast(level as int64) as level,
    COUNT(DISTINCT user_pseudo_id) as user_start
  FROM `royal-hexa-in-house.dung_dinner_flatten.start_level`
  WHERE version IN ('1.0.8', '1.0.9')
    AND (
      (version = '1.0.9' AND (event_date >= '2025-04-10' or event_date < '2025-05-01'))
    OR (version = '1.0.8' AND (event_date < '2025-04-10' or event_date >= '2025-05-01'))
    )
  GROUP BY  version, level
),
next_level_stats AS (
  SELECT 
    a.version,
    b.level - 1 as level,
    b.user_start as user_start_next_level
  FROM level_stats a join level_stats b
  on a.version = b.version
  and a.level = b.level - 1
)
select 
  a.version,
  a.level,
  a.user_start,
  case when b.user_start_next_level >= a.user_start then a.user_start - 1 else b.user_start_next_level end as user_start_next_level
 from level_stats a left join next_level_stats b
on  a.version = b.version
and a.level = b.level;

-- adrw_iap_level 
CREATE or replace TABLE `royal-hexa-in-house.dung_dinner_dashboard.iap_adsrw_level`
PARTITION BY event_date
CLUSTER BY level, version AS
with a as
(
  select event_date, version, level, count(distinct user_pseudo_id) as num_user
  from `royal-hexa-in-house.dung_dinner_flatten.start_level`
  group by 1,2,3
), b as 
(
  select event_date, version, level, count(user_pseudo_id) as num_ads
  from `royal-hexa-in-house.dung_dinner_flatten.level_adreward`
  group by 1,2,3
), c as 
(
  select event_date, version, level, count(user_pseudo_id) as num_iap 
  from `royal-hexa-in-house.dung_dinner_flatten.buy_iap`
  group by 1,2,3
)
select a.event_date, a.version, a.level, a.num_user, b.num_ads, c.num_iap
from a left join b on a.event_date = b.event_date and a.version = b.version and a.level = b.level
left join c on a.event_date = c.event_date and a.version = c.version and a.level = c.level;





-- start_drop
CREATE or replace TABLE `royal-hexa-in-house.dung_dinner_dashboard.start_drop`
CLUSTER BY level, version AS 
WITH level_stats AS (
  SELECT 
    version,
    CAST(level AS INT64) AS level,
    COUNT(DISTINCT user_pseudo_id) AS user_start
  FROM `royal-hexa-in-house.dung_dinner_flatten.start_level`
  WHERE version IN ('1.0.8', '1.0.9')
    AND (
      (version = '1.0.9' AND (event_date >= '2025-04-10' AND event_date < '2025-05-01'))
      OR (version = '1.0.8' AND (event_date < '2025-04-10' OR event_date >= '2025-05-01'))
    )
  GROUP BY version, level
)
SELECT
  *,
  SUM(CASE WHEN level = 1 THEN user_start ELSE 0 END) OVER (PARTITION BY version) AS start_level_1
FROM level_stats




