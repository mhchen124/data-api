package services

import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable
import scala.slick.jdbc.JdbcBackend._
import scala.math._
import scala.slick.util.SQLBuilder.Result
import scala.util.parsing.json.JSONObject
import javax.inject.Singleton

@Singleton
class PlainSqlRedshift extends App with RedshiftInterpolation with RedshiftTransfer with BuildRedshiftQuery {

    class Trends(val trendsData:List[KeyLongValuePair]) {
        private var viewsToday:KeyLongValuePair             = KeyLongValuePair("Empty", 0)
        private var viewsYesterday:KeyLongValuePair         = KeyLongValuePair("Empty", 0)
        private var reachToday:KeyLongValuePair             = KeyLongValuePair("Empty", 0)
        private var reachYesterday:KeyLongValuePair         = KeyLongValuePair("Empty", 0)
        private var avgViewTimeToday:KeyLongValuePair       = KeyLongValuePair("Empty", 0)
        private var avgViewTimeYesterday:KeyLongValuePair   = KeyLongValuePair("Empty", 0)
        private var likesToday:KeyLongValuePair             = KeyLongValuePair("Empty", 0)
        private var likesYesterday:KeyLongValuePair         = KeyLongValuePair("Empty", 0)
        private var sharesToday:KeyLongValuePair            = KeyLongValuePair("Empty", 0)
        private var sharesYesterday:KeyLongValuePair        = KeyLongValuePair("Empty", 0)
        private var commentsToday:KeyLongValuePair          = KeyLongValuePair("Empty", 0)
        private var commentsYesterday:KeyLongValuePair      = KeyLongValuePair("Empty", 0)

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

    class VideoRetention(val retentionStr:String) {
        private val retention:mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]()
        private val dataPairs = retentionStr.replace(""""""", "").replace("{", "").replace("}", "")
        private val arrPair = dataPairs.split(",")
        private var kvstring:Array[String] = null

        for (p <- arrPair) {
            kvstring = p.split(":")
            retention.put(kvstring(0).toInt,kvstring(1).toDouble)
        }

        private val max = retention.keys.max
        assert(max > 1)
        private val q1 = max / 4
        private val q2 = q1 + q1
        private val q3 = q2 + q1
        private val q4 = max

        val valQ1 = retention.get(q1).get
        val valQ2 = retention.get(q2).get
        val valQ3 = retention.get(q3).get
        val valQ4 = retention.get(q4).get
    }

    class VideoStatsGadgetData(val vids:String, val start: String, val stop: String) {

        private val arrIDs:Array[String] = vids.split(",")
        private val retentions:mutable.HashMap[String, VideoRetention] = new mutable.HashMap[String, VideoRetention]()
        private val views:mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
        private val avgTimes:mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

        private val retentionList = Database.forConfig("redshift") withSession { implicit session => queryVideoRetentionListBatch(session, vids)}
        retentionList.map(x => {retentions.put(x.k, new VideoRetention(x.v))})

        private val viewsList = Database.forConfig("redshift") withSession { implicit session => queryVideoViewsListDateRangeBatch(session, vids, start, stop)}
        viewsList.map(x => {views.put(x.k, x.v)})

        private val avgTimeList = Database.forConfig("redshift") withSession { implicit session => queryAverageTimeViewedListDateRangeBatch(session, vids, start, stop)}
        avgTimeList.map(x => {avgTimes.put(x.k, x.v)})

        var firstQuartile:Double = 0.0
        var secondQuartile:Double = 0.0
        var thirdQuartile:Double = 0.0
        var forthQuartile:Double = 0.0
        var avgTime:Long = 0

        private var totalViews:Long = 0
        private var totalViewsQ1:Long = 0
        private var totalViewsQ2:Long = 0
        private var totalViewsQ3:Long = 0
        private var totalViewsQ4:Long = 0

        for (vid <- arrIDs) {
            val v:Long = views.get(vid).get
            totalViews += v
            val ret:VideoRetention = retentions.get(vid).get
            totalViewsQ1 += (ret.valQ1 * v.toDouble).toLong
            totalViewsQ2 += (ret.valQ2 * v.toDouble).toLong
            totalViewsQ3 += (ret.valQ3 * v.toDouble).toLong
            totalViewsQ4 += (ret.valQ4 * v.toDouble).toLong
        }

        firstQuartile = totalViewsQ1.toDouble / totalViews.toDouble
        secondQuartile = totalViewsQ2.toDouble / totalViews.toDouble
        thirdQuartile = totalViewsQ3.toDouble / totalViews.toDouble
        forthQuartile = totalViewsQ4.toDouble / totalViews.toDouble

        for (vid <- arrIDs) {
            val ret:VideoRetention = retentions.get(vid).get
            val v:Long = views.get(vid).get
            val t:Long = avgTimes.get(vid).get
            avgTime += (t.toDouble * v.toDouble / totalViews.toDouble).toLong
        }

        override def toString = """{"AvgTimeViewed":""" + avgTime +
                                """, "Q1":""" + firstQuartile +
                                """, "Q2":""" + secondQuartile +
                                """, "Q3":""" + thirdQuartile +
                                """, "Q4":""" + forthQuartile + "}"
    }


    def testdb() : String = {
        Database.forConfig("redshift") withSession { implicit session => testFunc(session)}
    }


    // Project level

    def daoGetTotalVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_video_views")}
    }
    def daoGetTotalUniqueVideoViews(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_unique_video_views")}
    }
    def daoGetInteractions(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_actions")}
    }
    def daoGetTotalReachByGender(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_reach_page_posts")}
    }
    def daoGetTotalFollowers(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_followers") }
    }
    def daoGetTotalReach(projID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => querySingleLong(session, "total_reach")}
    }
    def daoGetTrendsData(projID: Long) : String = {
        val trendsData:List[KeyLongValuePair] =
            Database.forConfig("redshift") withSession { implicit session => queryKeyLongValuePairs(session, "seven_day_trends_data")}
        val myTrends = new Trends(trendsData)
        myTrends.toString
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


    // Asset level - daily data

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
    def daoGetDailyVideoViewsDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewsDateRange(session, videoID, start, stop)}
    }
    def daoGetDailyVideoViewTypesDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewTypesDateRange(session, videoID, start, stop)}
    }

    // Asset level - total number

    def daoGetVideoViewsDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoViewsDateRange(session, videoID, start, stop)}
    }
    def daoGetVideoReachDateRange(videoID: Long, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoReachDateRange(session, videoID, start, stop)}
    }
    def daoGetVideoRetention(videoID: Long) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoRetention(session, videoID)}
    }


    // Asset level - daily data batch APIs

    def daoGetAverageTimeViewedDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryAverageTimeViewedDateRangeBatch(session, videoIDs, start, stop)}
    }
    def daoGetDailyActionTypesDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyActionTypesDateRangeBatch(session, videoIDs, start, stop)}
    }
    def daoGetDailyReactionTypesDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyReactionTypesDateRangeBatch(session, videoIDs, start, stop)}
    }
    def daoGetDailyVideoViewTypesDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewTypesDateRangeBatch(session, videoIDs, start, stop)}
    }
    def daoGetDailyVideoViewsDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryDailyVideoViewsDateRangeBatch(session, videoIDs, start, stop)}
    }


    // Asset level - total number batch APIs

    def daoGetVideoViewsDateRangeBatch(videoIDs: String, start: String, stop: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoViewsDateRangeBatch(session, videoIDs, start, stop)}
    }
    def daoGetVideoRetentionBatch(videoIDs: String) : String = {
        Database.forConfig("redshift") withSession { implicit session => queryVideoRetentionBatch(session, videoIDs)}
    }


    def daoGetVideoStatsBatch(videoIDs: String, start: String, stop: String) : String = {
        val myStats = new VideoStatsGadgetData(videoIDs, start, stop)
        myStats.toString
    }
}
