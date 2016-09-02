package controllers

import javax.inject._
import play.api._
import play.api.mvc._

import services.ProjectStats

@Singleton
class ProjectStatsController @Inject() (pStats : ProjectStats) extends Controller {

    def testdb() = Action {
        Ok(pStats.testdb)
    }

    // Project level APIs

    def getTotalVideoViews(id : Long) = Action {
        Ok(pStats.totalVideoViews(id))
    }
    def getTotalUniqueVideoViews(id : Long) = Action {
        Ok(pStats.totalUniqueVideoViews(id))
    }
    def getTotalInteractions(id : Long) = Action {
        Ok(pStats.interactions(id))
    }
    def getTotalByGender(id : Long) = Action {
        Ok(pStats.gender(id))
    }
    def getTotalFollowers(id : Long) = Action {
        Ok(pStats.followers(id))
    }
    def getTotalReach(id : Long) = Action {
        Ok(pStats.reach(id))
    }
    def getTrends(id : Long) = Action {
        Ok(pStats.trends(id))
    }
    def getTotalPosts(id : Long) = Action {
        Ok(pStats.posts(id))
    }
    def getAverageTimeViewed(id : Long) = Action {
        Ok(pStats.averageTimeViewed(id))
    }
    def getTotalTimeViewed(id : Long) = Action {
        Ok(pStats.totalTimeViewed(id))
    }
    def getTotalVideoViewsDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.totalVideoViewsDateRange(id, start, stop))
    }
    def getTotalVideoViewTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.totalVideoViewTypesDateRange(id, start, stop))
    }
    def getTop10Heatmap(id : Long) = Action {
        Ok(pStats.videoTop10Heatmap(id))
    }
    def getTop10VideoIds(id : Long) = Action {
        Ok(pStats.videoTop10VideoIds(id))
    }

    // Asset level APIs - daily data

    def getDailyVideoReachDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoReachDateRange(id, start, stop))
    }
    def getAverageTimeViewedDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.averageTimeViewedDateRange(id, start, stop))
    }
    def getDailyActionTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyActionTypesDateRange(id, start, stop))
    }
    def getDailyReactionTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyReactionTypesDateRange(id, start, stop))
    }
    def getDailyVideoViewsDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewsDateRange(id, start, stop))
    }
    def getDailyVideoViewTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewTypesDateRange(id, start, stop))
    }

    // Asset level - total number

    def getVideoViewsDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.videoViewsDateRange(id, start, stop))
    }
    def getVideoReachDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.videoReachDateRange(id, start, stop))
    }

    def getVideoRetention(id : Long) = Action {
        Ok(pStats.videoRetention(id))
    }

    // Asset level APIs - batched assets calls for daily data

    def getAverageTimeViewedDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.averageTimeViewedDateRangeBatch(ids, start, stop))
    }
    def getDailyActionTypesDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.dailyActionTypesDateRangeBatch(ids, start, stop))
    }
    def getDailyReactionTypesDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.dailyReactionTypesDateRangeBatch(ids, start, stop))
    }
    def getDailyVideoViewsDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewsDateRangeBatch(ids, start, stop))
    }
    def getDailyVideoViewTypesDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewTypesDateRangeBatch(ids, start, stop))
    }

    // Asset level APIs - batched assets calls for total number

    def getVideoViewsDateRangeBatch(ids : String, start : String, stop : String) = Action {
        Ok(pStats.videoViewsDateRangeBatch(ids, start, stop))
    }
    def getVideoRetentionBatch(ids : String) = Action {
        Ok(pStats.videoRetentionBatch(ids))
    }


    def getVideoStatsBatch(ids : String, start: String, stop: String) = Action {
        Ok(pStats.videoStatsBatch(ids, start, stop))
    }
}
