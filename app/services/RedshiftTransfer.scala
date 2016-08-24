package services

import play.api.libs.json._
import scala.slick.jdbc.GetResult

/** The Data Transfer Objects for the PlainSqlRedshift */
trait RedshiftTransfer { this: PlainSqlRedshift =>

    case class GpsStats(end_time: String, plat_id: String, proj_id: String,
                        obj_type: String, stats_type: String, id: String,
                        title: String, name: String, value: String, proj_status: String)
    implicit val getGpsStatsResult = GetResult(r => GpsStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    case class IdTitleValueTime(id: String, title: String, value: String, sys_time: String)
    implicit val getIdTitleValueTimeResult = GetResult(r => IdTitleValueTime(r.<<, r.<<, r.<<, r.<<))
    implicit val IdTitleValueTimeReads = Json.reads[IdTitleValueTime]
    implicit val IdTitleValueTimeWrites = Json.writes[IdTitleValueTime]

    case class IdValueTime(id: String, value: String, sys_time: String)
    implicit val getIdValueTimeResult = GetResult(r => IdValueTime(r.<<, r.<<, r.<<))
    implicit val IdValueTimeReads = Json.reads[IdValueTime]
    implicit val IdValueTimeWrites = Json.writes[IdValueTime]

    case class TotalNumber(statNumber: Long)
    implicit val getTotalNumberResult = GetResult(r => TotalNumber(r.<<))

    case class KeyValuePair(k: String, v: Long)
    implicit val getKeyValueResult = GetResult(r => KeyValuePair(r.<<, r.<<))
}
