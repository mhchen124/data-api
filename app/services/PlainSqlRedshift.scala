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

    def daoGetTotalUniqueVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_unique_video_views")}
    }

    def daoGetInteractions(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_actions")}
    }

    def daoGetTotalReach(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_reach")}
    }

    def daoGetTotalReachByGender(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_reach_page_posts")}
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

    def daoGetTotalTimeViewed(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryNamedView(session, "total_time_viewed")}
    }

    def daoGetTotalVideoViewsDateRange(projID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryTotalVideoViewsDateRange(session, start, stop)}
    }

    def daoGetTotalVideoViewTypesDateRange(projID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryTotalVideoViewTypesDateRange(session, start, stop)}
    }
    def daoGetDailyVideoViewsDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewsDateRange(session, videoID, start, stop)}
    }
    def daoGetDailyVideoViewTypesDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewTypesDateRange(session, videoID, start, stop)}
    }
    def daoGetTop10Heatmap(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryTop10Heatmap(session)}
    }
    def daoGetDailyVideoReachDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoReachDateRange(session, videoID, start, stop)}
    }
    def daoGetVideoRetention(videoID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoRetention(session, videoID)}
    }
}
