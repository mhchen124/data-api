package services

import com.google.inject.Singleton
import play.api.libs.json.{JsValue, Json}

import scala.slick.jdbc.JdbcBackend._
import scala.slick.util.SQLBuilder.Result
import scala.util.parsing.json.JSONObject

@Singleton
class PlainSqlRedshift extends App with RedshiftInterpolation with RedshiftTransfer with BuildRedshiftQuery {

    class Trends(val trendsData:List[KeyValuePair]) {
        private var viewsToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var viewsYesterday:KeyValuePair = KeyValuePair("Empty", 0)
        private var reachToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var reachYesterday:KeyValuePair = KeyValuePair("Empty", 0)
        private var avgViewTimeToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var avgViewTimeYesterday:KeyValuePair = KeyValuePair("Empty", 0)
        private var likesToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var likesYesterday:KeyValuePair = KeyValuePair("Empty", 0)
        private var sharesToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var sharesYesterday:KeyValuePair = KeyValuePair("Empty", 0)
        private var commentsToday:KeyValuePair = KeyValuePair("Empty", 0)
        private var commentsYesterday:KeyValuePair = KeyValuePair("Empty", 0)

        trendsData.foreach(p => {
            p.k match {
                case "seven-day-reach today"                => reachToday.k = p.k; reachToday.v = p.v
                case "seven-day-reach yesterday"            => reachYesterday.k = p.k; reachYesterday.v = p.v
                case "seven-day-view-time today"            => avgViewTimeToday.k = p.k; avgViewTimeToday.v = p.v
                case "seven-day-view-time yesterday"        => avgViewTimeYesterday.k = p.k; avgViewTimeYesterday.v = p.v
                case "seven-day-views today"                => viewsToday.k = p.k; viewsToday.v = p.v
                case "seven-day-views yesterday"            => viewsYesterday.k = p.k; viewsYesterday.v = p.v
                case "seven_day_unique_comment today"       => commentsToday.k = p.k; commentsToday.v = p.v
                case "seven_day_unique_comment yesterday"   => commentsYesterday.k = p.k; commentsYesterday.v = p.v
                case "seven_day_unique_like today"          => likesToday.k = p.k; likesToday.v = p.v
                case "seven_day_unique_like yesterday"      => likesYesterday.k = p.k; likesYesterday.v = p.v
                case "seven_day_unique_share today"         => sharesToday.k = p.k; sharesToday.v = p.v
                case "seven_day_unique_share yesterday"     => sharesYesterday.k = p.k; sharesYesterday.v = p.v
                case _ => println("Unknown case!")
            }
        })

        def getViewsTrend:Double = (viewsToday.v - viewsYesterday.v)/(viewsYesterday.v.toDouble)
        def getReachTrend:Double = (reachToday.v - reachYesterday.v)/(reachYesterday.v.toDouble)
        def getViewsTimeTrend:Double = (avgViewTimeToday.v - avgViewTimeYesterday.v)/(avgViewTimeYesterday.v.toDouble)
        def getActionsTrend:Double = {
            val actionsToday:Double = commentsToday.v + likesToday.v + sharesToday.v
            val actionsYesterday:Double = commentsYesterday.v + likesYesterday.v + sharesYesterday.v
            (actionsToday - actionsYesterday)/actionsYesterday
        }
        override def toString = {
            """{"Views Trend":""" + getViewsTrend + """, "Reach Trend":""" + getReachTrend + """, "AvgTime Trend":""" + getViewsTimeTrend + """, "Actions Trend":""" + getActionsTrend + "}"
        }
    }

    def testdb() : String = {
        Database.forConfig("redshift") withSession { implicit session => testFunc(session)}
    }

    def daoGetTotalVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_video_views")}
    }

    def daoGetTotalUniqueVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_unique_video_views")}
    }

    def daoGetInteractions(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_actions")}
    }

    def daoGetTotalReach(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_reach")}
    }

    def daoGetTotalReachByGender(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_reach_page_posts")}
    }

    def daoGetTotalFollowers(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_followers") }
    }

    def daoGetTotalPosts(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_posts")}
    }

    def daoGetAvgTimeViewed(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "avg_time_viewed")}
    }

    def daoGetTotalTimeViewed(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_time_viewed")}
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
    def daoGetTop10VideoIds(projID: Long) : String = {
        val vidList = Database.forConfig("redshift") withSession { implicit session => queryTop10VideoIds(session)}
        val start = """"{"ranked_video_ids(id, sum)":[{ """
        val end = """}]}"""
        val json = vidList.mkString(start, ",", end)
        json.toString()
    }
    def daoGetDailyVideoReachDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoReachDateRange(session, videoID, start, stop)}
    }
    def daoGetAverageTimeViewedDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryAverageTimeViewedDateRange(session, videoID, start, stop)}
    }
    def daoGetDailyActionTypesDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyActionTypesDateRange(session, videoID, start, stop)}
    }
    def daoGetDailyReactionTypesDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyReactionTypesDateRange(session, videoID, start, stop)}
    }
    def daoGetVideoRetention(videoID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoRetention(session, videoID)}
    }
    def daoGetTrendsData(projID: Long) : String = {
        val trendsData:List[KeyValuePair] =
            Database.forConfig("redshift") withSession { implicit session => queryKeyValuePairs(session, "seven_day_trends_data")}
        val myTrends = new Trends(trendsData)
        myTrends.toString
    }
}
