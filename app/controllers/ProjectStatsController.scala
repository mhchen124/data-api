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
    def getDailyVideoViewsDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewsDateRange(id, start, stop))
    }
    def getDailyVideoViewTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.dailyVideoViewTypesDateRange(id, start, stop))
    }
    def getTop10Heatmap(id : Long) = Action {
        Ok(pStats.videoTop10Heatmap(id))
    }
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
    def getVideoRetention(id : Long) = Action {
        Ok(pStats.videoRetention(id))
    }
}
