package services

import play.api.libs.json._

import scala.slick.jdbc.JdbcBackend.Session
import scala.slick.jdbc.StaticQuery

trait BuildRedshiftQuery { this: PlainSqlRedshift =>

    def testFunc(implicit session: Session): String = {
        println("Testing: KV value returned by DB...")
        "OK"
    }

    def querySingleLong(implicit session: Session, view_name: String, postClause: String = "") : String = {
        val numberFromView = StaticQuery[Int, StatLong] + "SELECT sum FROM " + view_name + " " + postClause + " limit ? ;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromView(1).first.statNumber)))
    }
    def queryKeyLongValuePairs(implicit session: Session, view_name: String, postClause: String = "") : List[KeyLongValuePair] = {
        val kvPairs = StaticQuery.queryNA[KeyLongValuePair]("SELECT * FROM " + view_name + " " + postClause + ";")
        kvPairs.list
    }
    def queryKeyStrValuePairs(implicit session: Session, view_name: String, postClause: String = "") : List[KeyStrValuePair] = {
        val kvPairs = StaticQuery.queryNA[KeyStrValuePair]("SELECT * FROM " + view_name + " " + postClause + ";")
        kvPairs.list
    }

    // PROJECT LEVEL CALLs

    def queryTotalVideoViews(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat LIKE '" + projID.toString +
            "' AND title LIKE 'Lifetime Total Video Views' AND sys_time >= (CURRENT_DATE-2)  GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalUniqueVideoViews(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND proj_id_plat like '" + projID.toString +
            "' AND title LIKE 'Lifetime Unique Video Views' AND sys_time >= (CURRENT_DATE-2)  GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryInteractions(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT ((SELECT sum FROM total_reactions_" + projID.toString + ") + (SELECT sum FROM total_share_comments_" + projID.toString + ")) AS sum;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalFollowers(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT value AS sum FROM fb_insights WHERE title LIKE 'page_fan_count' AND proj_id_plat LIKE '" + projID.toString +
            "' AND end_time >= (CURRENT_DATE-2) ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalReach(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] + "SELECT SUM(value) AS sum FROM fb_insights " +
            "WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Video Total Reach' " +
            "AND proj_id_plat LIKE '" + projID.toString + "' AND sys_time >= (CURRENT_DATE-2) GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTrendsData(implicit session: Session, projID: Long) : List[KeyLongValuePair] = {
        val queryResult = StaticQuery[KeyLongValuePair] + "SELECT * FROM  seven_day_trends_data_" +  projID.toString + ";"
        queryResult.list
    }
    def queryTotalPosts(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Number of posts made by the admin' AND proj_id_plat LIKE '" + projID.toString + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryAvgTimeViewed(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT AVG(value)/1000 AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time >= (CURRENT_DATE-2) AND proj_id_plat LIKE '" +
            projID.toString + "' GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalTimeViewed(implicit session: Session, projID: Long) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value)/1000 AS sum FROM fb_insights WHERE stats_type = 'VideoInsights' AND title = 'Lifetime Total Video View Time (in MS)' AND sys_time >= (CURRENT_DATE-2) AND proj_id_plat LIKE '" +
            projID.toString + "' GROUP BY sys_time ORDER BY sys_time DESC LIMIT 1;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryTotalVideoViewsDateRange(implicit session: Session, projID: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Video Views' " +
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
        val queryResult = StaticQuery[IdValueTime] + "SELECT * FROM top10_heatmap_" + projID.toString
        Json.toJson(queryResult.list).toString()
    }
    def queryTop10VideoIds(implicit session: Session, projID: Long) : List[KeyLongValuePair] = {
        val queryResult = StaticQuery[KeyLongValuePair] +
            "SELECT asset_id_plat, SUM(value) FROM top10_heatmap_" + projID.toString + " GROUP BY asset_id_plat ORDER BY SUM(value) DESC;"
        queryResult.list

    }


    // ASSET LEVEL CALLs - Daily Data

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

    // ASSET LEVEL CALLs - Total Numbers

    def queryVideoViewsDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryVideoReachDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND asset_id_plat LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }
    def queryVideoRetention(implicit session: Session, vid: Long) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' " +
            "AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time >= (CURRENT_DATE-2) AND asset_id_plat LIKE '" +
            vid.toString + "%' ORDER BY sys_time DESC LIMIT 1;"
        Json.toJson(queryResult.list).toString()
    }


    // BATCHED ASSET LEVEL CALLs - Daily Data

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
//            "SELECT asset_id_plat, SUM(value) FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > '" +
//            start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ") GROUP BY asset_id_plat;"
            "WITH ld AS (SELECT asset_id_plat, max(sys_time) AS latest FROM fb_insights where stats_type = 'VideoInsights' GROUP BY asset_id_plat) " +
            "SELECT s.asset_id_plat, s.value FROM fb_insights s JOIN ld ON ld.asset_id_plat = s.asset_id_plat " +
            "WHERE s.sys_time = ld.latest AND s.stats_type = 'VideoInsights' AND s.title LIKE 'Lifetime Average time video viewed' AND s.asset_id_plat IN (" + InCondition + ");"
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
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Organic Video Views' " +
                "OR title LIKE 'Daily Paid Video Views') AND sys_time > '" +
                start + "' AND sys_time < '" + stop + "' AND asset_id_plat IN (" + InCondition + ");")
        Json.toJson(queryResult.list).toString()
    }


    // BATCHED ASSET LEVEL CALLs - Total numbers

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
            "WITH ld AS (SELECT asset_id_plat, max(sys_time) AS latest FROM fb_insights where stats_type = 'VideoInsights' GROUP BY asset_id_plat) " +
            "SELECT s.asset_id_plat, s.value FROM fb_insights s JOIN ld ON ld.asset_id_plat = s.asset_id_plat " +
            "WHERE s.sys_time = ld.latest AND s.stats_type = 'VideoInsights' AND s.title LIKE 'Lifetime Percentage of viewers at each interval%' AND s.asset_id_plat IN (" + InCondition + ");"
        queryResult.list
    }
    def queryVideoRetentionBatch(implicit session: Session, vids: String) : String = {
        Json.toJson(queryVideoRetentionListBatch(session, vids)).toString()
    }

}
