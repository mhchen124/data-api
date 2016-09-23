/*
-- ########## Showoff Stuff ##############

DROP VIEW session_view;
CREATE VIEW session_view AS (
SELECT * FROM showoff_events
  ORDER BY session_id, current_page);

CREATE VIEW completion_view AS ( -- UPDATE THIS
SELECT * FROM showoff_events 
--  WHERE total_pages > 1 AND current_page = total_pages
WHERE event_type = 'TriviaCompleted'
);
  
CREATE VIEW dropoff_view AS (
SELECT event_type, 
    show_title,
    game_id, 
    player_id, 
    MAX(current_page) AS max_page,
    total_pages,
    MAX(DATE(date_created)) as date_created,
    session_id
FROM showoff_events
WHERE current_page >= 0 AND (event_type = 'TriviaStageForward' OR event_type = 'TriviaStart') -- UPDATED THIS
GROUP BY session_id, event_type, show_title, game_id, player_id, total_pages
ORDER BY show_title);

CREATE VIEW dropoff_detail_view AS ( -- UPDATED THIS
SELECT event_type, 
    show_title,
    game_id, 
    player_id, 
    current_page,
    total_pages,
    DATE(date_created) as date_created,
    session_id
FROM showoff_events
WHERE current_page < total_pages and current_page >= 0 AND (event_type = 'TriviaStageForward' OR event_type = 'TriviaStart')
ORDER BY date_created, show_title, current_page);

CREATE VIEW completed_sid_view AS ( -- UPDATED THIS
SELECT date_created, session_id FROM showoff_events 
WHERE event_type='TriviaCompleted' AND session_id <> '');

CREATE VIEW play_random_view AS (
select * from showoff_events where event_type = 'PlayRandomShowTrivia');

CREATE VIEW play_random_complete_view AS (
select * from showoff_events where event_type = 'PlayRandomShowTrivia' and session_id in (
  select session_id from completed_sid_view)
);

CREATE VIEW play_same_view AS (
select * from showoff_events where event_type = 'PlaySameShowNewTrivia');

CREATE VIEW play_same_complete_view AS (
select * from showoff_events where event_type = 'PlaySameShowNewTrivia' and session_id in (
  select session_id from completed_sid_view)
);

-- ############# Facebook stats ###############

CREATE TABLE IF NOT EXISTS fb_insights (
  end_time    VARCHAR(64),
  plat_id     VARCHAR(64),
  proj_id     VARCHAR(64),
  proj_status VARCHAR(64),
  id          VARCHAR(256),
  title       VARCHAR(256),
  name        VARCHAR(256),
  value       VARCHAR(2048),
  sys_time    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gps_insights (
  end_time    VARCHAR(64),
  plat_id     VARCHAR(64),
  proj_id     VARCHAR(64),
  proj_status VARCHAR(64),
  obj_type    VARCHAR(16),
  stats_type  VARCHAR(16),
  id          VARCHAR(256),
  title       VARCHAR(256),
  name        VARCHAR(256),
  value       VARCHAR(2048)
  sys_time    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gps_proj_assets (
  proj_id         VARCHAR(32),
  asset_id        VARCHAR(32),
  plat_id         VARCHAR(32),
  proj_id_plat    VARCHAR(64),
  asset_id_plat   VARCHAR(100),
  proj_status     VARCHAR(16),
  asset_status    VARCHAR(16),
  obj_type        VARCHAR(16),
  stats_type      VARCHAR(64),
  start_time      VARCHAR(64),
  end_time        VARCHAR(64),
  ops_status      VARCHAR(16),
  sys_time        TIMESTAMP
);

CREATE VIEW page_video_views AS
select * from fb_insights where name='page_video_views' and title='Daily Total Video Views' order by end_time;

CREATE VIEW page_views_unique AS
select * from fb_insights where name='page_views_unique' and title='Daily Logged-in Page Views' order by end_time;

CREATE VIEW page_views_total AS
select * from fb_insights where name='page_views_total' and title='Daily Total views count per Page' order by end_time;


CREATE VIEW union_view AS
select * from page_video_views
UNION
select * from page_views_unique
UNION
select * from page_views_total;


-- ##############################
-- #### DATA API 1.0 query views ####
-- ##############################

DROP VIEW total_unique_video_views;
CREATE VIEW total_unique_video_views AS
SELECT 'total_unique_video_views' AS name, SUM(CONVERT(int, value)) AS sum FROM fb_insights WHERE title LIKE 'Lifetime Unique Video Views';

drop view total_video_views;
CREATE VIEW total_video_views AS
select 'total_video_views' as name, SUM(CONVERT(int, value)) AS sum from fb_insights where title like 'Lifetime Total Video Views';

drop view total_interactions;
CREATE VIEW total_interactions AS
SELECT 'total reactions' AS name,
  SUM( CONVERT(int, json_extract_path_text(value, 'love')) + 
    CONVERT(int, json_extract_path_text(value, 'haha')) +
    CONVERT(int, json_extract_path_text(value, 'like')) +
    CONVERT(int, json_extract_path_text(value, 'sorry')) +
    CONVERT(int, json_extract_path_text(value, 'anger')) +
    CONVERT(int, json_extract_path_text(value, 'wow')) ) AS sum
FROM fb_insights WHERE title LIKE 'Lifetime Reactions by type';

drop view total_reach_page_posts;
CREATE VIEW total_reach_page_posts AS
select 'total_reach_page_posts' as name,sum(value) from fb_insights where title like 'Daily Reach Of Page Posts';

drop view total_followers;
CREATE VIEW total_followers AS
select 'total_followers' as name,sum(value) from fb_insights where title like 'Daily New Follows';

drop view total_reach;
CREATE VIEW total_reach AS
select 'total_reach' as name,sum(value) from fb_insights where title = 'Daily Total Reach';

drop view total_posts;
CREATE VIEW total_posts AS
SELECT 'total_posts' AS name, sum(value) FROM fb_insights WHERE title LIKE 'Daily Number of posts made by the admin';
SELECT 'total_posts' AS name, sum(value) FROM fb_insights WHERE title LIKE 'Daily Number of posts made by the admin' and proj_id_plat like '1495821923975356';

drop view avg_time_viewed;
CREATE VIEW avg_time_viewed AS
SELECT 'avg_time_viewed (sec)' AS name, AVG(value)/1000 AS sum FROM fb_insights WHERE title LIKE 'Lifetime Average time video viewed';
SELECT 'avg_time_viewed (sec)' AS name, AVG(value)/1000 AS sum FROM fb_insights WHERE title LIKE 'Lifetime Average time video viewed' and proj_id_plat like '1495821923975356';

drop view total_time_viewed;
CREATE VIEW total_time_viewed AS
select 'total_time_viewed (sec)'  as name, SUM(value)/1000 as sum from fb_insights where title = 'Lifetime Total Video View Time (in MS)';
select 'total_time_viewed (sec)' as name, SUM(value)/1000 as sum from fb_insights where title = 'Lifetime Total Video View Time (in MS)' and proj_id_plat like '1495821923975356';

drop view video_views_date_range;
CREATE VIEW video_views_date_range AS
select 'video_views_date_range'  as name, sum(value) from fb_insights where title = 'Daily Total Video Views' and end_time > '?' and end_time < '?';

drop view video_view_types_date_range;
CREATE VIEW video_view_types_date_range AS
SELECT 'Total Promoted Views' AS name, SUM(value) AS sum FROM fb_insights WHERE title LIKE 'Daily Total Promoted Views' AND end_time < '2016-07-07' AND end_time > '2016-06-15'
UNION
SELECT 'Total Organic Views' AS name, SUM(value) AS sum FROM fb_insights WHERE title LIKE 'Daily Total Organic Views' AND end_time < '2016-07-07' AND end_time > '2016-06-15';

drop view total_video_impressions;
CREATE VIEW total_video_impressions AS
select 'total_video_impressions' as name, SUM(CONVERT(int, value)) AS sum from fb_insights where title like 'Lifetime Video Total Impressions';


-- ##############################
-- ########### Data API 2.0
-- ##############################

-- ## Total Reach
CREATE VIEW total_reach AS
SELECT 'daily_aggr_total_reach' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Reach';
SELECT 'lifetime_video_total_reach' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Video Total Reach' AND proj_id_plat like '1495821923975356' AND sys_time > (CURRENT_DATE-2);

-- ## Total Followers
CREATE VIEW total_followers AS
SELECT 'daily_aggr_total_followers' AS name, SUM(value) AS sum FROM fb_insights WHERE title LIKE '%Daily total post like reactions of a page%';
SELECT 'total_followers' AS name, end_time, value AS sum FROM fb_insights WHERE title LIKE 'page_fan_count' AND proj_id_plat like '1495821923975356' AND sys_time > (CURRENT_DATE-2);

-- ## Total Video Views
CREATE VIEW total_video_views AS
--SELECT 'daily_aggr_total_video_views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views';
--SELECT 'lifetime_total_video_views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Total Video Views' AND sys_time > (CURRENT_DATE-2);
SELECT 'lifetime_total_video_views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '1495821923975356' AND title LIKE 'Lifetime Total Video Views' AND sys_time > (CURRENT_DATE-2);

-- ## Total Unique Views
CREATE VIEW total_unique_video_views AS
SELECT 'daily_aggr_total_unique_video_views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Unique Video Views%';
SELECT 'lifetime_total_unique_video_views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Unique Video Views' AND sys_time > (CURRENT_DATE-2);

-- ## Trending section data

-- ## Separated Trends APIs: reach
DROP VIEW IF EXISTS seven_day_reach_delta;
CREATE VIEW seven_day_reach_delta AS
SELECT 'seven-day-reach today' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Reach' AND proj_id_plat LIKE '1495821923975356' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE)
UNION ALL
SELECT 'seven-day-reach yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Reach' AND proj_id_plat LIKE '1495821923975356' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);

-- ## Separated Trends APIs: views #####################################
DROP VIEW IF EXISTS seven_day_views_delta;
CREATE VIEW seven_day_views_delta AS
SELECT 'seven-day-views today' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '1495821923975356' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE)
UNION ALL
SELECT 'seven-day-views yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '1495821923975356' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);

-- ## Separate Trends APIs: avg view time
DROP VIEW IF EXISTS seven_day_view_time_delta;
CREATE VIEW seven_day_view_time_delta AS
SELECT 'seven-day-view-time today' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '1495821923975356' AND sys_time > (CURRENT_DATE-7) AND sys_time < (CURRENT_DATE)
UNION ALL
SELECT 'seven-day-view-time yesterday' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '1495821923975356' AND sys_time > (CURRENT_DATE-8) AND sys_time < (CURRENT_DATE-1);

-- ## Separate Trends APIs: actions
DROP VIEW IF EXISTS seven_day_unique_share_today;
create view seven_day_unique_share_today as
select 'seven_day_unique_share today' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> ''
union all
select 'seven_day_unique_like today' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> ''
union all
select 'seven_day_unique_comment today' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';

DROP VIEW IF EXISTS seven_day_unique_share_yesterday;
create view seven_day_unique_share_yesterday as
select 'seven_day_unique_share yesterday' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> ''
union all
select 'seven_day_unique_like yesterday' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> ''
union all
select 'seven_day_unique_comment yesterday' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '1495821923975356' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';

DROP VIEW IF EXISTS seven_day_unique_actions_delta;
create view seven_day_unique_actions_delta as
select * from seven_day_unique_share_today
union all
select * from seven_day_unique_share_yesterday;

DROP VIEW IF EXISTS seven_day_trends_data;
CREATE VIEW seven_day_trends_data AS
select * from seven_day_reach_delta
union all
select * from seven_day_views_delta
union all
select * from seven_day_view_time_delta
union all
select * from seven_day_unique_actions_delta order by name;
--------- ############################################################


-- ## Total Actions

DROP VIEW IF EXISTS total_reactions;
CREATE VIEW total_reactions AS
SELECT 'total reactions' AS name,
  SUM( CONVERT(int, json_extract_path_text(value, 'love')) +
    CONVERT(int, json_extract_path_text(value, 'haha')) +
    CONVERT(int, json_extract_path_text(value, 'like')) +
    CONVERT(int, json_extract_path_text(value, 'sorry')) +
    CONVERT(int, json_extract_path_text(value, 'anger')) +
    CONVERT(int, json_extract_path_text(value, 'wow')) ) AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Reactions by type') AND proj_id_plat LIKE '1495821923975356' AND sys_time > (CURRENT_DATE-2);

DROP VIEW IF EXISTS total_share_comments;
CREATE VIEW total_share_comments AS
SELECT 'total share and comments' AS name,
  SUM(
    nullif(json_extract_path_text(value, 'share'), ' ')::int +
    nullif(json_extract_path_text(value, 'comment'), ' ')::int )
  AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Video Stories by action type') AND proj_id_plat LIKE '1495821923975356' AND sys_time > (CURRENT_DATE-2);

DROP VIEW IF EXISTS total_actions;
CREATE VIEW total_actions AS
SELECT 'total_actions' AS name, ((SELECT sum FROM total_reactions) + (SELECT sum FROM total_share_comments)) AS sum;

-- ## Top 10 heat map data (by 10-sec views)

CREATE VIEW top10_video_ids AS
-- select id, sum(value) from top10_heatmap group by id order by sum(value) desc;
select asset_id_plat, sum(value) from top10_heatmap where proj_id_plat like '1495821923975356' group by asset_id_plat order by sum(value) desc;

CREATE VIEW top10_heatmap AS
/*
SELECT id, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE and sys_time > CURRENT_DATE-8 AND id IN
  (SELECT id FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE AND sys_time > CURRENT_DATE-8 GROUP BY id ORDER BY SUM(value) DESC LIMIT 10)
  ORDER BY id;
*/
SELECT asset_id_plat, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE and sys_time > CURRENT_DATE-8 AND asset_id_plat IN
  (SELECT asset_id_plat FROM fb_insights WHERE stats_type = 'DailyDiff' AND proj_id_plat like '1495821923975356' AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE AND sys_time > CURRENT_DATE-8 GROUP BY asset_id_plat ORDER BY SUM(value) DESC LIMIT 10)
  ORDER BY asset_id_plat;

-- ## Usage: Video Views by day for a video over a date range
SELECT id, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Total Video Views' AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';

-- ## Usage: Video Views by day by type for a video over a date range
SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Organic Video Views' OR title LIKE 'Daily Paid Video Views') AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';

-- ## Usage: Video reach by day for a video over a date range
SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';

-- ## Usage: Avg View Time
SELECT * FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Average time video viewed') AND id LIKE '2054005264823683%' order by sys_time;

-- ## Video retention
SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time > (CURRENT_DATE-2) AND id LIKE '2054005264823683%';

-- ## Video stats view
-- Same as above retention, use that data

-- ## Usage: Video Total Actions by day by type for a video over a date range
SELECT 'daily_actions' AS name
  ( CONVERT(int, json_extract_path_text(value, 'love')) +
    CONVERT(int, json_extract_path_text(value, 'haha')) +
    CONVERT(int, json_extract_path_text(value, 'like')) +
    CONVERT(int, json_extract_path_text(value, 'sorry')) +
    CONVERT(int, json_extract_path_text(value, 'anger')) +
    CONVERT(int, json_extract_path_text(value, 'wow')) ) AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Daily Reactions by type') AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';

-- ## Video Actions by day by type over date range
SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Video Stories by action type') AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';

-- ## Usage: Video Total Reactions by day by type for a video over a date range
SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Reactions by type') AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8) AND id LIKE '2054005264823683%';


-- ########## Batch Calls ########

-- SELECT asset_id_plat, sum(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Total Video Views' AND sys_time < (CURRENT_DATE) AND sys_time > (CURRENT_DATE-8)
-- AND asset_id_plat IN ('2054005264823683','2031862120371331','2048932098664333','2048959688661574','2050079491882927') group by asset_id_plat;


DROP VIEW union_numbers;
CREATE VIEW union_numbers AS
SELECT name, sum FROM total_video_views
UNION
SELECT name, sum  FROM total_interactions
UNION 
SELECT name, sum  FROM total_reach_page_posts
UNION
SELECT name, sum  FROM total_followers
UNION
SELECT name, sum  FROM total_reach
UNION
SELECT name, sum  FROM total_posts
UNION
SELECT name, sum  FROM avg_time_viewed
UNION
SELECT name, sum  FROM total_time_viewed
UNION
SELECT name, sum FROM total_unique_video_views
UNION
SELECT name, sum FROM total_video_impressions;


-- ##### Backup table

drop table fb_insights_backup;
create table fb_insights_backup as
select * from fb_insights;

*/

/*

-- #### DW Copy Operation #######



-- ### Update to set Project IDs
/*
UPDATE fb_insights SET proj_id_plat = '612330242258452' WHERE stats_type = 'VideoInsights' AND asset_id_plat IN
  (SELECT asset_id_plat FROM gps_proj_assets WHERE proj_id_plat = '612330242258452');
UPDATE fb_insights SET proj_id_plat = '612330242258452' WHERE stats_type = 'DailyDiff' AND asset_id_plat IN
  (SELECT asset_id_plat FROM gps_proj_assets WHERE proj_id_plat = '612330242258452');

*/



-- #### GPS stats data #####

-- to gps_stats table

COPY gps_stats(end_time, id, title, name, value, proj_id, proj_status, obj_type, stats_type) FROM 's3://gps-stats/2016/07/13/fb-graph-1712909435617295_1749700138604891-insights-2016-07-13.data' 
  CREDENTIALS 'aws_access_key_id=AKIAILESYVWIW6RGB7AA;aws_secret_access_key=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' 
  json 'auto';
  
-- to fb_insights table

COPY fb_insights(end_time, id, title, name, value, proj_id, proj_status, obj_type, stats_type) FROM 's3://gps-stats/2016/07/13/fb-graph-1712909435617295_1749700138604891-insights-2016-07-13.data' 
  CREDENTIALS 'aws_access_key_id=AKIAILESYVWIW6RGB7AA;aws_secret_access_key=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' 
  json 'auto';

*/

-- select end_time, sys_time, value FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Daily Reactions by type') AND sys_time < (CURRENT_DATE-2) AND sys_time > (CURRENT_DATE-3) AND id LIKE '2054005264823683%';
-- update fb_insights set asset_id_plat = split_part(id, '/', 1) where (stats_type = 'VideoInsights' or stats_type = 'DailyDiff');


----#########################
----######## CSST enhancement


--- ####### total views #####
--SELECT SUM(value) as sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '612330242258452' AND title LIKE 'Lifetime Total Video Views' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

--- ####### total views #####
-- SELECT SUM(value) as sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '612330242258452' AND title LIKE 'Lifetime Unique Video Views' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

--- ####### interactions
/*
DROP VIEW IF EXISTS total_actions;
DROP VIEW IF EXISTS total_reactions;
CREATE VIEW total_reactions AS SELECT 'total reactions' AS name, SUM( CONVERT(int, json_extract_path_text(value, 'love')) +
  CONVERT(int, json_extract_path_text(value, 'haha')) + CONVERT(int, json_extract_path_text(value, 'like')) +
  CONVERT(int, json_extract_path_text(value, 'sorry'))  +
  CONVERT(int, json_extract_path_text(value, 'anger'))  +
  CONVERT(int, json_extract_path_text(value, 'wow')) )
  AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Reactions by type') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;


DROP VIEW IF EXISTS total_share_comments;
CREATE VIEW total_share_comments AS SELECT 'total share and comments' AS name, SUM( nullif(json_extract_path_text(value, 'share'), ' ')::int + nullif(json_extract_path_text(value, 'comment'), ' ')::int ) AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Video Stories by action type') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

SELECT ((SELECT sum FROM total_reactions) + (SELECT sum FROM total_share_comments)) AS sum;
*/

-- ########## Total followers
--- SELECT value AS sum FROM fb_insights WHERE title LIKE 'page_fan_count' AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) ORDER BY sys_time DESC LIMIT 1;

-- ########## Total reach

--SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Video Total Reach' AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

-- Total posts
-- SELECT value FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Number of posts made by the admin' AND proj_id_plat LIKE '612330242258452';

-- Avg time viewed
-- SELECT AVG(value)/1000 AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > (CURRENT_DATE-2) AND proj_id_plat LIKE '612330242258452' GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

-- Total time viewed
-- SELECT AVG(value)/1000 AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Total Video View Time (in MS)' AND sys_time > (CURRENT_DATE-2) AND proj_id_plat LIKE '612330242258452' GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

-- top10-heatmap


-- ###### Top10 ids
-- SELECT asset_id_plat, SUM(value) FROM top10_heatmap WHERE proj_id_plat LIKE '612330242258452' GROUP BY asset_id_plat ORDER BY SUM(value) DESC;


/*
DROP VIEW IF EXISTS top10_heatmap_612330242258452;
CREATE VIEW top10_heatmap_612330242258452 AS SELECT asset_id_plat, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff'
  AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE and sys_time > CURRENT_DATE-8 AND asset_id_plat IN
  (SELECT asset_id_plat FROM fb_insights WHERE stats_type = 'DailyDiff' AND proj_id_plat LIKE '612330242258452' AND title LIKE '%Unique 10-Second Views%'
  AND sys_time < CURRENT_DATE AND sys_time > CURRENT_DATE-8 GROUP BY asset_id_plat ORDER BY SUM(value) DESC LIMIT 10)  ORDER BY asset_id_plat;

*/

-- SELECT sum(value) FROM fb_insights WHERE stats_type = 'DailyDiff' and proj_id_plat LIKE '612330242258452'
--  AND title LIKE '%Unique 10-Second Views%' AND sys_time like '%2016-09-17%';

-- ###### Top10 ids
-- SELECT asset_id_plat, SUM(value) FROM top10_heatmap_612330242258452 GROUP BY asset_id_plat ORDER BY SUM(value) DESC;

-- ###### Top10-heatmap
-- SELECT * FROM top10_heatmap_612330242258452;

-- ###### Trends
/*
DROP VIEW IF EXISTS seven_day_reach_delta_612330242258452;
CREATE VIEW seven_day_reach_delta_612330242258452 AS
  SELECT 'seven-day-reach today' AS name, SUM(value) AS sum FROM fb_insights
  WHERE stats_type = 'Insights' AND title = 'Daily Total Reach' AND proj_id_plat LIKE '612330242258452' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) UNION ALL
  SELECT 'seven-day-reach yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights'
  AND title = 'Daily Total Reach' AND proj_id_plat LIKE '612330242258452' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);

DROP VIEW IF EXISTS seven_day_views_delta_612330242258452;
CREATE VIEW seven_day_views_delta_612330242258452 AS
SELECT 'seven-day-views today' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '612330242258452' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE)
UNION ALL
SELECT 'seven-day-views yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '612330242258452' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);

-- ## Separate Trends APIs: avg view time
DROP VIEW IF EXISTS seven_day_view_time_delta_612330242258452;
CREATE VIEW seven_day_view_time_delta_612330242258452 AS
SELECT 'seven-day-view-time today' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-7) AND sys_time < (CURRENT_DATE)
UNION ALL
SELECT 'seven-day-view-time yesterday' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-8) AND sys_time < (CURRENT_DATE-1);

-- ## Separate Trends APIs: actions
DROP VIEW IF EXISTS seven_day_unique_share_today_612330242258452;
create view seven_day_unique_share_today_612330242258452 as
select 'seven_day_unique_share today' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> ''
union all
select 'seven_day_unique_like today' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> ''
union all
select 'seven_day_unique_comment today' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';

DROP VIEW IF EXISTS seven_day_unique_share_yesterday_612330242258452;
create view seven_day_unique_share_yesterday_612330242258452 as
select 'seven_day_unique_share yesterday' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> ''
union all
select 'seven_day_unique_like yesterday' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> ''
union all
select 'seven_day_unique_comment yesterday' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '612330242258452' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';

DROP VIEW IF EXISTS seven_day_unique_actions_delta_612330242258452;
create view seven_day_unique_actions_delta_612330242258452 as
select * from seven_day_unique_share_today_612330242258452
union all
select * from seven_day_unique_share_yesterday_612330242258452;

DROP VIEW IF EXISTS seven_day_trends_data_612330242258452;
CREATE VIEW seven_day_trends_data_612330242258452 AS
select * from seven_day_reach_delta_612330242258452
union all
select * from seven_day_views_delta_612330242258452
union all
select * from seven_day_view_time_delta_612330242258452
union all
select * from seven_day_unique_actions_delta_612330242258452 order by name;
*/

--- ###### action/reaction
/*
DROP VIEW IF EXISTS total_reactions_612330242258452;
CREATE VIEW total_reactions_612330242258452 AS
SELECT 'total reactions' AS name, SUM( CONVERT(int, json_extract_path_text(value, 'love')) +
  CONVERT(int, json_extract_path_text(value, 'haha')) + CONVERT(int, json_extract_path_text(value, 'like')) +
  CONVERT(int, json_extract_path_text(value, 'sorry'))  +
  CONVERT(int, json_extract_path_text(value, 'anger'))  +
  CONVERT(int, json_extract_path_text(value, 'wow')) )
  AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Reactions by type') AND proj_id_plat LIKE '612330242258452' AND sys_time >= (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

DROP VIEW IF EXISTS total_share_comments_612330242258452;
CREATE VIEW total_share_comments_612330242258452 AS
SELECT 'total share and comments' AS name, SUM( nullif(json_extract_path_text(value, 'share'), ' ')::int + nullif(json_extract_path_text(value, 'comment'), ' ')::int ) AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Video Stories by action type') AND proj_id_plat LIKE '612330242258452' AND sys_time >= (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;
*/



-------- 1487009051605323
-------- 1495821923975356
-------- 612330242258452

--- ############ Action reaction

CREATE VIEW total_reactions_612330242258452 AS
SELECT 'total reactions' AS name, SUM( CONVERT(int, json_extract_path_text(value, 'love')) +
  CONVERT(int, json_extract_path_text(value, 'haha')) + CONVERT(int, json_extract_path_text(value, 'like')) +
  CONVERT(int, json_extract_path_text(value, 'sorry'))  +
  CONVERT(int, json_extract_path_text(value, 'anger'))  +
  CONVERT(int, json_extract_path_text(value, 'wow')) )
  AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Reactions by type') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;

CREATE VIEW total_share_comments_612330242258452 AS
SELECT 'total share and comments' AS name, SUM( nullif(json_extract_path_text(value, 'share'), ' ')::int + nullif(json_extract_path_text(value, 'comment'), ' ')::int ) AS sum
FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Video Stories by action type') AND proj_id_plat LIKE '612330242258452' AND sys_time > (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;
