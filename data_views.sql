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

drop view avg_time_viewed;
CREATE VIEW avg_time_viewed AS
SELECT 'avg_time_viewed (sec)' AS name, AVG(value)/1000 AS sum FROM fb_insights WHERE title LIKE 'Lifetime Average time video viewed';

drop view total_time_viewed;
CREATE VIEW total_time_viewed AS
select 'total_time_viewed (sec)'  as name, SUM(value)/1000 as sum from fb_insights where title = 'Lifetime Total Video View Time (in MS)';

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
-- ########### Data API 2.0 tasks
-- ##############################

-- ## Total Reach (same as 1.0)
CREATE VIEW total_reach AS
SELECT 'total_reach' AS name,sum(value) FROM fb_insights WHERE title = 'Daily Total Reach';

-- ## Video reach (lifetime measure)
-- SELECT * FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LI 'Lifetime Video Total Reach';

-- ## Total Video Views (same as 1.0)
CREATE VIEW total_video_views AS
SELECT 'total_video_views' AS name, SUM(CONVERT(int, value)) AS sum FROM fb_insights WHERE title LIKE 'Lifetime Total Video Views';

-- ## Total Unique Views (same as 1.0)
CREATE VIEW total_unique_video_views AS
SELECT 'total_unique_video_views' AS name, SUM(CONVERT(int, value)) AS sum FROM fb_insights WHERE title LIKE 'Lifetime Unique Video Views';

-- ## Treading section data (TBD)

-- ## Total Followers
CREATE VIEW total_followers AS
SELECT 'total_followers' AS name, SUM(value) FROM fb_insights WHERE title LIKE 'Daily total post like reactions of a page';

-- ## Top 10 heat map data 
-- SELECT id, SUM(CONVERT(int,value)) FROM fb_insights WHERE stats_type = 'VideoInsights' AND title = 'Lifetime Unique 10-Second Views' ORDER BY CONVERT(int,value) DESC LIMIT 10;

-- ## Usage: Video Views
-- select * from fb_insights where stats_type = 'VideoInsights' and title like 'Lifetime Total Video Views' order by convert(int,value) desc limit 10;

-- ## Video retention
-- select * from fb_insights where stats_type = 'VideoInsights' and title like 'Lifetime Percentage of viewers at each interval (Video )'; 

-- ## Video stats view
-- Same as above retention, use that data



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

-- Backup table

CREATE TABLE fb_insights_bak AS
SELECT * FROM fb_insights;

*/

/*

-- #### DW Copy Operation #######

-- [Page insights data]

COPY fb_insights (end_time, id, title, name, value, obj_type, stats_type, sys_time) FROM 's3://gps-stats/2016/08/11/fb-graph-1495821923975356-insights-2016-08-11DailyStandard.data' 
  CREDENTIALS 'aws_access_key_id=AKIAILESYVWIW6RGB7AA;aws_secret_access_key=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' 
  json 'auto';
           
-- [Video insights data]

COPY fb_insights(end_time, id, title, name, value, proj_id, proj_status, obj_type, stats_type, sys_time) FROM 's3://gps-stats/2016/08/11/fb-graph-1495821923975356-2016-08-11.video_insights.data' 
  CREDENTIALS 'aws_access_key_id=AKIAILESYVWIW6RGB7AA;aws_secret_access_key=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' 
  json 'auto';
  
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

