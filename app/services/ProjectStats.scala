package services

import javax.inject.Inject

import play.api.mvc._



/**
  * Trait for all project stats collecting components to extend.
  */
trait ProjectStats {
    def testdb() : String
    def totalVideoViews(projectID : Long) : String
    def totalUniqueVideoViews(projectID : Long) : String
    def interactions(projectID : Long) : String
    def gender(projectID : Long) : String
    def followers(projectID : Long) : String
    def reach(projectID : Long) : String
    def posts(projectID : Long) : String
    def averageTimeViewed(projectID : Long) : String
    def totalTimeViewed(projectID : Long) : String
    def totalVideoViewsDateRange(id : Long, start : String, stop : String) : String
    def totalVideoViewTypesDateRange(id : Long, start : String, stop : String) : String
    def dailyVideoViewsDateRange(id : Long, start : String, stop : String) : String
    def dailyVideoViewTypesDateRange(id : Long, start : String, stop : String) : String
    def videoTop10Heatmap(projectID : Long) : String
    def videoTop10VideoIds(projectID : Long) : String
    def dailyVideoReachDateRange(id : Long, start : String, stop : String) : String
    def averageTimeViewedDateRange(id : Long, start : String, stop : String) : String
    def dailyActionTypesDateRange(id : Long, start : String, stop : String) : String
    def dailyReactionTypesDateRange(id : Long, start : String, stop : String) : String
    def videoRetention(id : Long) : String
    def trends(projectID : Long) : String
}

/**
  * An inplementation of project stats component for GPS FB stats.
  */
class GpsProjectFacebookStats @Inject() (theDAL : PlainSqlRedshift) extends ProjectStats {
    def testdb() : String = {
        theDAL.testdb()
    }
    def totalVideoViews(projectID : Long) : String = {
        theDAL.daoGetTotalVideoViews(projectID)
    }
    def totalUniqueVideoViews(projectID : Long) : String = {
        theDAL.daoGetTotalUniqueVideoViews(projectID)
    }
    def interactions(projectID : Long) : String = {
        theDAL.daoGetInteractions(projectID)
    }
    def gender(projectID : Long) : String = {
        theDAL.daoGetTotalReachByGender(projectID)
    }
    def followers(projectID : Long) : String = {
        theDAL.daoGetTotalFollowers(projectID)
    }
    def reach(projectID : Long) : String = {
        theDAL.daoGetTotalReach(projectID)
    }
    def posts(projectID : Long) : String = {
        theDAL.daoGetTotalPosts(projectID)
    }
    def averageTimeViewed(projectID : Long) : String = {
        theDAL.daoGetAvgTimeViewed(projectID)
    }
    def totalTimeViewed(projectID : Long) : String = {
        theDAL.daoGetTotalTimeViewed(projectID)
    }
    def totalVideoViewsDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetTotalVideoViewsDateRange(projectID, start, stop)
    }
    def totalVideoViewTypesDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetTotalVideoViewTypesDateRange(projectID, start, stop)
    }
    def dailyVideoViewsDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewsDateRange(projectID, start, stop)
    }
    def dailyVideoViewTypesDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewTypesDateRange(projectID, start, stop)
    }
    def videoTop10Heatmap(projectID : Long) : String = {
        theDAL.daoGetTop10Heatmap(projectID)
    }
    def videoTop10VideoIds(projectID : Long) : String = {
        theDAL.daoGetTop10VideoIds(projectID)
    }
    def dailyVideoReachDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoReachDateRange(projectID, start, stop)
    }
    def averageTimeViewedDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetAverageTimeViewedDateRange(projectID, start, stop)
    }
    def dailyActionTypesDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyActionTypesDateRange(projectID, start, stop)
    }
    def dailyReactionTypesDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyReactionTypesDateRange(projectID, start, stop)
    }
    def videoRetention(id : Long) : String = {
        theDAL.daoGetVideoRetention(id)
    }
    def trends(projectID : Long) : String = {
        theDAL.daoGetTrendsData(projectID)

    }
}
