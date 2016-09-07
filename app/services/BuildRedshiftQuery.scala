package services

import play.api.libs.json._

import scala.slick.jdbc.JdbcBackend.Session
import scala.slick.jdbc.StaticQuery

trait BuildRedshiftQuery { this: PlainSqlRedshift =>

    def testFunc(implicit session: Session): String = {
        println("Testing: KV value returned by DB...")
        val res = queryKeyLongValuePairs(session, "seven_day_reach_delta")
        res.toString()
    }

    def querySingleLong(implicit session: Session, view_name: String, postClause: String = "") : String = {
        val numberFromView = StaticQuery[Int, StatLong] + "select sum from " + view_name + " " + postClause + " limit ? ;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromView(1).first.statNumber)))
    }
    def queryKeyLongValuePairs(implicit session: Session, view_name: String, postClause: String = "") : List[KeyLongValuePair] = {
        val kvPairs = StaticQuery.queryNA[KeyLongValuePair]("select * from " + view_name + " " + postClause + ";")
        kvPairs.list
    }
    def queryKeyStrValuePairs(implicit session: Session, view_name: String, postClause: String = "") : List[KeyStrValuePair] = {
        val kvPairs = StaticQuery.queryNA[KeyStrValuePair]("select * from " + view_name + " " + postClause + ";")
        kvPairs.list
    }

    // Project level calls

    def queryTotalVideoViews(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '" + projID.toString + "' AND title LIKE 'Lifetime Total Video Views' AND sys_time > (CURRENT_DATE-2);"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalUniqueVideoViews(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '" + projID.toString + "' AND title LIKE 'Lifetime Unique Video Views' AND sys_time > (CURRENT_DATE-2);"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryInteractions(implicit session: Session, projID: Long) : String = {
        val reactions = StaticQuery + "DROP VIEW IF EXISTS total_reactions; " +
            "CREATE VIEW total_reactions AS SELECT 'total reactions' AS name, SUM( CONVERT(int, json_extract_path_text(value, 'love')) " +
            "CONVERT(int, json_extract_path_text(value, 'haha')) " +
            "CONVERT(int, json_extract_path_text(value, 'like')) " +
            "CONVERT(int, json_extract_path_text(value, 'sorry')) " +
            "CONVERT(int, json_extract_path_text(value, 'anger')) " +
            "CONVERT(int, json_extract_path_text(value, 'wow')) ) AS sum FROM fb_insights "
        "WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Reactions by type') AND proj_id_plat LIKE '" + projID.toString + "' AND sys_time > (CURRENT_DATE-2);"

        val shareComments = StaticQuery + "DROP VIEW IF EXISTS total_share_comments;" +
            "CREATE VIEW total_share_comments AS SELECT 'total share and comments' AS name, " +
            "SUM( nullif(json_extract_path_text(value, 'share'), ' ')::int + nullif(json_extract_path_text(value, 'comment'), ' ')::int ) AS sum " +
            "FROM fb_insights WHERE stats_type = 'VideoInsights' AND (title LIKE 'Lifetime Video Stories by action type') AND proj_id_plat LIKE '" + projID.toString + "' AND sys_time > (CURRENT_DATE-2);"

        val numberFromQuery = StaticQuery[StatLong] + "SELECT ((SELECT sum FROM total_reactions) + (SELECT sum FROM total_share_comments)) AS sum;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalFollowers(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT value AS sum FROM fb_insights WHERE title LIKE 'page_fan_count' AND proj_id_plat LIKE '" + projID.toString + "' AND sys_time > (CURRENT_DATE-2);"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalReach(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT SUM(value) AS sum FROM fb_insights " +
            "WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Video Total Reach' " +
            "AND proj_id_plat LIKE '" + projID.toString + "' AND sys_time > (CURRENT_DATE-2);"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTrendsData(implicit session: Session, projID: Long) : List[KeyLongValuePair] = {

        val reach = StaticQuery + "DROP VIEW IF EXISTS seven_day_reach_delta;" + "" +
            "CREATE VIEW seven_day_reach_delta AS " +
            "SELECT 'seven-day-reach today' AS name, SUM(value) AS sum FROM fb_insights " +
            "WHERE stats_type = 'Insights' AND title = 'Daily Total Reach' AND proj_id_plat LIKE '" + projID.toString +
            "' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) UNION ALL " +
            "SELECT 'seven-day-reach yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' " +
            "AND title = 'Daily Total Reach' AND proj_id_plat LIKE '" + projID.toString +
            "' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);"

        val views = StaticQuery + "DROP VIEW IF EXISTS seven_day_views_delta; CREATE VIEW seven_day_views_delta AS SELECT 'seven-day-views today' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '" + projID.toString +
        "' AND end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) UNION ALL SELECT 'seven-day-views yesterday' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Video Views' AND proj_id_plat LIKE '" + projID.toString +
        "' AND end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1);"

        val avgTime = StaticQuery + "DROP VIEW IF EXISTS seven_day_view_time_delta; CREATE VIEW seven_day_view_time_delta AS SELECT 'seven-day-view-time today' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '" + projID.toString +
        "' AND sys_time > (CURRENT_DATE-7) AND sys_time < (CURRENT_DATE) UNION ALL SELECT 'seven-day-view-time yesterday' AS name, CONVERT(int,SUM(value)/7000) AS sum FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE '%Total Video View Time%') AND proj_id_plat LIKE '" + projID.toString +
            "' AND sys_time > (CURRENT_DATE-8) AND sys_time < (CURRENT_DATE-1);"

        val actionToday = StaticQuery + "DROP VIEW IF EXISTS seven_day_unique_share_today; create view seven_day_unique_share_today as select 'seven_day_unique_share today' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> '' union all select 'seven_day_unique_like today' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> '' union all select 'seven_day_unique_comment today' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-7) AND end_time < (CURRENT_DATE) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';"

        val actionYesterday = StaticQuery + "DROP VIEW IF EXISTS seven_day_unique_share_yesterday; create view seven_day_unique_share_yesterday as select 'seven_day_unique_share yesterday' as name, sum(json_extract_path_text(value, 'share')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_admin_num_posts_by_type%' and json_extract_path_text(value, 'share') <> '' union all select 'seven_day_unique_like yesterday' as name, sum(json_extract_path_text(value, 'like')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'like') <> '' union all select 'seven_day_unique_comment yesterday' as name, sum(json_extract_path_text(value, 'comment')) as sum from fb_insights where stats_type = 'Insights' AND proj_id_plat LIKE '" + projID.toString +
        "' and end_time > (CURRENT_DATE-8) AND end_time < (CURRENT_DATE-1) and name like '%page_positive_feedback_by_type_unique%' and json_extract_path_text(value, 'comment') <> '';"

        val actionDelta = StaticQuery + "DROP VIEW IF EXISTS seven_day_unique_actions_delta; create view seven_day_unique_actions_delta as select * from seven_day_unique_share_today union all select * from seven_day_unique_share_yesterday;"

        val sevenDayTrends = StaticQuery + "DROP VIEW IF EXISTS seven_day_trends_data; " +
            "CREATE VIEW seven_day_trends_data AS select * from seven_day_reach_delta " +
            "union all select * from seven_day_views_delta " +
            "union all select * from seven_day_view_time_delta " +
            "union all select * from seven_day_unique_actions_delta order by name;"

        val queryResult = StaticQuery[KeyLongValuePair] + "SELECT * FROM  seven_day_trends_data;"
        queryResult.list
    }
    def queryTotalPosts(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT sum(value) FROM fb_insights WHERE title LIKE 'Daily Number of posts made by the admin' AND proj_id_plat LIKE '" + projID.toString + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryAvgTimeViewed(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT AVG(value)/1000 AS sum FROM fb_insights WHERE title LIKE 'Lifetime Average time video viewed' AND proj_id_plat LIKE '" + projID.toString + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalTimeViewed(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "select SUM(value)/1000 as sum from fb_insights where title = 'Lifetime Total Video View Time (in MS)' AND proj_id_plat LIKE '" + projID.toString + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalVideoViewsDateRange(implicit session: Session, projID: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Video Views' " +
            "AND proj_id_plat LIKE '" + projID.toString + "' AND end_time > '" + start + "' AND end_time < '" + stop + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalVideoViewTypesDateRange(implicit session: Session, projID: Long, start: String, stop: String) : String = {
        var jTmp : JsArray = new JsArray()
        val numberFromQuery = StaticQuery.queryNA[KeyLongValuePair](
            "SELECT 'Total Promoted Views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights'" +
                " AND proj_id_plat LIKE '" + projID.toString + "' AND title LIKE 'Daily Total Promoted Views' AND end_time < '" +
                stop + "' AND end_time > '" + start + "' UNION " +
            "SELECT 'Total Organic Views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights'" +
                " AND proj_id_plat LIKE '" + projID.toString + "' AND title LIKE 'Daily Total Organic Views' AND end_time < '" +
                stop + "' AND end_time > '" + start + "';")
        var sb: StringBuilder = new StringBuilder("[")
        numberFromQuery foreach { c =>
            println("* " + c.k + "\t" + c.v)
            sb.append(JsObject(Seq("name" -> JsString(c.k), "count" -> JsNumber(c.v))))
            sb.append(",")
        }
        sb.delete(sb.length-1, sb.length)
        sb.append("]")
        Json.prettyPrint(Json.parse(sb.toString()))
    }
    def queryTop10Heatmap(implicit session: Session, projID: Long) : String = {
        val queryResult = StaticQuery[IdValueTime] + "SELECT asset_id_plat, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' " +
            "AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE and sys_time > CURRENT_DATE-8 AND asset_id_plat IN " +
            "(SELECT asset_id_plat FROM fb_insights WHERE stats_type = 'DailyDiff' AND proj_id_plat like '" + projID.toString +
            "' AND title LIKE '%Unique 10-Second Views%' AND sys_time < CURRENT_DATE AND sys_time > CURRENT_DATE-8 " +
            "GROUP BY asset_id_plat ORDER BY SUM(value) DESC LIMIT 10)  ORDER BY asset_id_plat;"
        Json.toJson(queryResult.list).toString()
    }
    def queryTop10VideoIds(implicit session: Session, projID: Long) : List[KeyLongValuePair] = {
        val queryResult = StaticQuery[KeyLongValuePair] +
            "select asset_id_plat, sum(value) from top10_heatmap where proj_id_plat like '" + projID.toString + "' group by asset_id_plat order by sum(value) desc;"
        queryResult.list
    }


    // Asset level - daily data

    def queryDailyVideoReachDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }
    def queryAverageTimeViewedDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyActionTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Stories by action type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyReactionTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Reactions by type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyVideoViewsDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyVideoViewTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery.queryNA[IdTitleValueTime](
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Organic Video Views' OR title LIKE 'Daily Paid Video Views') AND sys_time > '" +
                start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';")
        Json.toJson(queryResult.list).toString()
    }

    // Asset level - total number

    def queryVideoViewsDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryVideoReachDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryVideoRetention(implicit session: Session, vid: Long) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time > (CURRENT_DATE-2) AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }


    // Batched asset APIs - daily data

    def queryAverageTimeViewedDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");"
        Json.toJson(queryResult.list).toString()
    }
    def queryAverageTimeViewedListDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : List[KeyLongValuePair] = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[KeyLongValuePair] +
            "SELECT asset_id_plat, SUM(value) FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ") GROUP BY asset_id_plat;"
        queryResult.list
    }
    def queryDailyActionTypesDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Stories by action type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyReactionTypesDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Reactions by type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyVideoViewsDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");"
        Json.toJson(queryResult.list).toString()
    }
    def queryDailyVideoViewTypesDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery.queryNA[IdTitleValueTime](
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Organic Video Views' OR title LIKE 'Daily Paid Video Views') AND sys_time > '" +
                start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");")
        Json.toJson(queryResult.list).toString()
    }


    // Batched asset APIs - total number

    def queryVideoViewsListDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : List[KeyLongValuePair] = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[KeyLongValuePair] +
            "SELECT asset_id_plat, SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ") GROUP BY asset_id_plat;"
        queryResult.list
    }
    def queryVideoViewsDateRangeBatch(implicit session: Session, vids: String, start: String, stop: String) : String = {
        Json.toJson(queryVideoViewsListDateRangeBatch(session, vids, start, stop)).toString()
    }
    def queryVideoRetentionListBatch(implicit session: Session, vids: String) : List[KeyStrValuePair] = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[KeyStrValuePair] +
            "SELECT asset_id_plat, value FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time > (CURRENT_DATE-2) AND asset_id_plat IN (" + InCondition + ");"
        queryResult.list
    }
    def queryVideoRetentionBatch(implicit session: Session, vids: String) : String = {
        Json.toJson(queryVideoRetentionListBatch(session, vids)).toString()
    }

}
