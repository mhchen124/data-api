package services

import play.api.libs.json._

import scala.slick.jdbc.JdbcBackend.Session
import scala.slick.jdbc.StaticQuery

trait BuildRedshiftQuery { this: PlainSqlRedshift =>

    def printAll(implicit session: Session): Unit = {
        println("Insights:")
        StaticQuery.queryNA[GpsStats]("select * from gps_stats limit 10") foreach { c =>
            println("* " + c.name + "\t" + c.title + "\t" + c.value + "\t" + c.obj_type + "\t" + c.stats_type)
        }
    }

    def queryNamedView(implicit session: Session, view_name: String, postClause: String = "") : String = {
        val numberFromView = StaticQuery[Int, TotalNumber] + "select sum from " + view_name + " " + postClause + " limit ?"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromView(1).first.statNumber)))
    }

    def queryVideoViewsDateRange(implicit session: Session, start: String, stop: String) : String = {
        val numberFromQuery = StaticQuery[TotalNumber] +
            "SELECT SUM(value) FROM fb_insights WHERE title = 'Daily Total Video Views' AND end_time > '" + start + "' and end_time < '" + stop + "'"
        Json.prettyPrint(Json.arr(Json.obj("count" -> numberFromQuery.first.statNumber)))
    }

    def queryVideoViewTypesDateRange(implicit session: Session, start: String, stop: String) : String = {

        var jTmp : JsArray = new JsArray()

        val numberFromQuery = StaticQuery.queryNA[KeyValuePair](
            "SELECT 'Total Promoted Views' AS name, SUM(value) AS sum FROM fb_insights WHERE title LIKE 'Daily Total Promoted Views' AND end_time < '" + stop + "' AND end_time > '" + start + "' UNION " +
            "SELECT 'Total Organic Views' AS name, SUM(value) AS sum FROM fb_insights WHERE title LIKE 'Daily Total Organic Views' AND end_time < '" + stop + "' AND end_time > '" + start + "';")

        var sb: StringBuilder = new StringBuilder("[")

        numberFromQuery foreach { c =>
            println("* " + c.k + "\t" + c.v)
            sb.append(JsObject(Seq("name" -> JsString(c.k), "count" -> JsNumber(c.v))))
            sb.append(",")
        }
        sb.delete(sb.length-1, sb.length)
        sb.append("]")
        //Json.prettyPrint(Json.arr(Json.obj("name" -> numberFromQuery().first.k, "count" -> numberFromQuery().first.v)))
        //sb.toString()
        Json.prettyPrint(Json.parse(sb.toString()))
    }

    def queryTop10Heatmap(implicit session: Session) : String = {
        val heatmap = StaticQuery[IdValueTime] + "SELECT * FROM top10_heatmap"
        Json.toJson(heatmap.list).toString()
    }
}
