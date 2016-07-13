package services

import scala.slick.jdbc.JdbcBackend.Session
import scala.slick.jdbc.StaticQuery.interpolation

trait RedshiftInterpolation { this: PlainSqlRedshift =>

    def totalVideoViews(projID: Long)(implicit session: Session): Option[TotalNumber] =
        sql"select sum from total_video_views".as[TotalNumber].firstOption
}
