package services

import play.api.libs.json._

import scala.slick.jdbc.JdbcBackend.Session
import scala.slick.jdbc.StaticQuery

trait BuildRedshiftQuery { this: PlainSqlRedshift =>

    def testFunc(implicit session: Session): String = {
        println("Testing: KV value returned by DB...")
        val res = queryKeyValuePairs(session, "seven_day_reach_delta")
        res.toString()
    }

    def querySingleLong(implicit session: Session, view_name: String, postClause: String = "") : String = {
        val numberFromView = StaticQuery[Int, StatLong] + "select sum from " + view_name + " " + postClause + " limit ? ;"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromView(1).first.statNumber)))
    }

    def queryKeyValuePairs(implicit session: Session, view_name: String, postClause: String = "") : List[KeyValuePair] = {
        val kvPairs = StaticQuery.queryNA[KeyValuePair]("select * from " + view_name + " " + postClause + ";")
        kvPairs.list
    }

    def queryTotalVideoViewsDateRange(implicit session: Session, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'Insights' AND title = 'Daily Total Video Views' AND end_time > '" +
            start + "' AND end_time < '" + stop + "';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }

    def queryTotalVideoViewTypesDateRange(implicit session: Session, start: String, stop: String) : String = {
        var jTmp : JsArray = new JsArray()
        val numberFromQuery = StaticQuery.queryNA[KeyValuePair](
            "SELECT 'Total Promoted Views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Promoted Views' AND end_time < '" +
                stop + "' AND end_time > '" + start + "' UNION " +
            "SELECT 'Total Organic Views' AS name, SUM(value) AS sum FROM fb_insights WHERE stats_type = 'Insights' AND title LIKE 'Daily Total Organic Views' AND end_time < '" +
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

    def queryDailyVideoViewsDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryVideoViewsDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title = 'Daily Total Video Views' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }

    def queryDailyVideoViewTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery.queryNA[IdTitleValueTime](
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND (title LIKE 'Daily Organic Video Views' OR title LIKE 'Daily Paid Video Views') AND sys_time > '" +
                start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';")
        Json.toJson(queryResult.list).toString()
    }

    def queryTop10Heatmap(implicit session: Session) : String = {
        val queryResult = StaticQuery[IdValueTime] + "SELECT * FROM top10_heatmap;"
        Json.toJson(queryResult.list).toString()
    }

    def queryTop10VideoIds(implicit session: Session) : List[KeyValuePair] = {
        val queryResult = StaticQuery[KeyValuePair] + "SELECT * FROM top10_video_ids;"
        queryResult.list
    }

    def queryDailyVideoReachDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryVideoReachDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[StatLong] +
            "SELECT SUM(value) FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Total Reach' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }

    def queryAverageTimeViewedDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Average time video viewed' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryDailyActionTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Video Stories by action type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryDailyReactionTypesDateRange(implicit session: Session, vid: Long, start: String, stop: String) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'DailyDiff' AND title LIKE 'Daily Reactions by type' AND sys_time > '" +
            start + "' AND sys_time < '" + stop + "' AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryVideoRetention(implicit session: Session, vid: Long) : String = {
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT id, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time > (CURRENT_DATE-1) AND id LIKE '" + vid.toString + "%';"
        Json.toJson(queryResult.list).toString()
    }

    def queryVideoRetention(implicit session: Session, vids: String) : String = {
        val idList:Array[String] = vids.split(",").map(x => "'" + x + "'")
        val InCondition:String = idList.mkString(",")
        val queryResult = StaticQuery[IdTitleValueTime] +
            "SELECT asset_id_plat, title, value, sys_time FROM fb_insights WHERE stats_type = 'VideoInsights' AND title LIKE 'Lifetime Percentage of viewers at each interval%' AND sys_time > (CURRENT_DATE-1) AND asset_id_plat IN (" + InCondition + ");"
        Json.toJson(queryResult.list).toString()
    }

}
