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

    def queryNamedView(implicit session: Session, name: String) : String = {
        val numberFromView = StaticQuery[Int, TotalNumber] + "select sum from " + name + " limit ?"
        val retJObj = Json.obj("count" -> numberFromView(1).first.statNumber)
        retJObj.toString
    }

}
