package services

import com.google.inject.Singleton

import scala.slick.jdbc.JdbcBackend._
import scala.slick.util.SQLBuilder.Result

@Singleton
class PlainSqlRedshift extends App with RedshiftInterpolation with RedshiftTransfer with BuildRedshiftQuery {

    def testdb() : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_reach")}
    }

    def daoGetTotalVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_video_views")}
    }

    def daoGetInteractions(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_interaction")}
    }

    def daoGetTotalReach(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_reach")}
    }

    def daoGetTotalReachByGender(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_reach_by_gender")}
    }

    def daoGetTotalFollowers(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_followers") }
    }

    def daoGetTotalPosts(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_posts")}
    }

    def daoGetAvgTimeViewed(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "avg_time_viewed")}
    }

    def daoGetTotalVideoViewDateRange(projID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_video_view_date_range")}
    }

    def daoGetTotalVideoViewTypeDateRange(projID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_video_view_types_date_range")}
    }
}
