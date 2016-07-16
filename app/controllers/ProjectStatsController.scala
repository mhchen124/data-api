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
    def getVideoViewsDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.videoViewsDateRange(id, start, stop))
    }
    def getVideoViewTypesDateRange(id : Long, start : String, stop : String) = Action {
        Ok(pStats.videoViewTypesDateRange(id, start, stop))
    }
}
