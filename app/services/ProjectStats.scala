package services

import javax.inject.Inject

import play.api.mvc._


/**
  * Trait for all project stats collecting components to extend.
  */
trait ProjectStats {
    def testdb() : String
    def totalVideoViews(projectID : Long) : String
    def interactions(projectID : Long) : String
    def gender(projectID : Long) : String
    def followers(projectID : Long) : String
    def reach(projectID : Long) : String
    def posts(projectID : Long) : String
    def averageTimeViewed(projectID : Long) : String
    def totalTimeViewed(projectID : Long) : String
    def videoViewsDateRange(id : Long, start : String, stop : String) : String
    def videoViewTypesDateRange(id : Long, start : String, stop : String) : String
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
    def videoViewsDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetTotalVideoViewsDateRange(projectID, start, stop)
    }
    def videoViewTypesDateRange(projectID : Long, start : String, stop : String) : String = {
        theDAL.daoGetTotalVideoViewTypesDateRange(projectID, start, stop)
    }
}
