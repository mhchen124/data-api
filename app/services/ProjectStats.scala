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
    def trends(projectID : Long) : String
    def posts(projectID : Long) : String
    def averageTimeViewed(projectID : Long) : String
    def totalTimeViewed(projectID : Long) : String
    def totalVideoViewsDateRange(id : Long, start : String, stop : String) : String
    def totalVideoViewTypesDateRange(id : Long, start : String, stop : String) : String
    def videoTop10Heatmap(projectID : Long) : String
    def videoTop10VideoIds(projectID : Long) : String

    def averageTimeViewedDateRange(id : Long, start : String, stop : String) : String
    def dailyActionTypesDateRange(id : Long, start : String, stop : String) : String
    def dailyReactionTypesDateRange(id : Long, start : String, stop : String) : String
    def dailyVideoViewsDateRange(id : Long, start : String, stop : String) : String
    def dailyVideoViewTypesDateRange(id : Long, start : String, stop : String) : String
    def dailyVideoReachDateRange(id : Long, start : String, stop : String) : String

    def videoViewsDateRange(id : Long, start : String, stop : String) : String
    def videoReachDateRange(id : Long, start : String, stop : String) : String
    def videoRetention(id : Long) : String


    def averageTimeViewedDateRangeBatch(ids : String, start : String, stop : String) : String
    def dailyActionTypesDateRangeBatch(ids : String, start : String, stop : String) : String
    def dailyReactionTypesDateRangeBatch(ids : String, start : String, stop : String) : String
    def dailyVideoViewsDateRangeBatch(ids : String, start : String, stop : String) : String
    def dailyVideoViewTypesDateRangeBatch(ids : String, start : String, stop : String) : String


    def videoViewsDateRangeBatch(ids : String, start : String, stop : String) : String
    def videoRetentionBatch(ids : String) : String


    def videoStatsBatch(ids : String, start: String, stop: String) : String
}

/**
  * An inplementation of project stats component for GPS FB stats.
  */
class GpsProjectFacebookStats @Inject() (theDAL : PlainSqlRedshift) extends ProjectStats {
    def testdb() : String = {
        theDAL.testdb()
    }

    // Project level APIs

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
    def trends(projectID : Long) : String = {
        theDAL.daoGetTrendsData(projectID)
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
    def videoTop10Heatmap(projectID : Long) : String = {
        theDAL.daoGetTop10Heatmap(projectID)
    }
    def videoTop10VideoIds(projectID : Long) : String = {
        theDAL.daoGetTop10VideoIds(projectID)
    }


    // Asset level APIs - daily data

    def dailyVideoReachDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoReachDateRange(assetID, start, stop)
    }
    def averageTimeViewedDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetAverageTimeViewedDateRange(assetID, start, stop)
    }
    def dailyActionTypesDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyActionTypesDateRange(assetID, start, stop)
    }
    def dailyReactionTypesDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyReactionTypesDateRange(assetID, start, stop)
    }
    def dailyVideoViewsDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewsDateRange(assetID, start, stop)
    }
    def dailyVideoViewTypesDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewTypesDateRange(assetID, start, stop)
    }


    // Asset level - total number

    def videoViewsDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetVideoViewsDateRange(assetID, start, stop)
    }
    def videoReachDateRange(assetID : Long, start : String, stop : String) : String = {
        theDAL.daoGetVideoReachDateRange(assetID, start, stop)
    }
    def videoRetention(assetID : Long) : String = {
        theDAL.daoGetVideoRetention(assetID)
    }


    // Asset level APIs - batched assets calls for daily data

    def averageTimeViewedDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetAverageTimeViewedDateRangeBatch(assetIDs, start, stop)
    }
    def dailyActionTypesDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetDailyActionTypesDateRangeBatch(assetIDs, start, stop)
    }
    def dailyReactionTypesDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetDailyReactionTypesDateRangeBatch(assetIDs, start, stop)
    }
    def dailyVideoViewTypesDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewTypesDateRangeBatch(assetIDs, start, stop)
    }
    def dailyVideoViewsDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetDailyVideoViewsDateRangeBatch(assetIDs, start, stop)
    }


    // Asset level APIs - batched assets calls for total number

    def videoViewsDateRangeBatch(assetIDs : String, start : String, stop : String) : String = {
        theDAL.daoGetVideoViewsDateRangeBatch(assetIDs, start, stop)
    }
    def videoRetentionBatch(assetIDs : String) : String = {
        theDAL.daoGetVideoRetentionBatch(assetIDs)
    }


    def videoStatsBatch(assetIDs : String, start: String, stop: String) : String = {
        theDAL.daoGetVideoStatsBatch(assetIDs, start, stop)
    }
}
