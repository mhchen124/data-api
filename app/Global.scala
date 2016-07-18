import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.twirl.api.Html

import scala.concurrent.Future

object Global extends GlobalSettings {

    override def onStart(app: Application) {
        Logger.info("Application has started")
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

    override def onError(request: RequestHeader, ex: Throwable) = {
        Future.successful(InternalServerError(
            views.html.main("DataAPI Error: ") {new Html("Got an error " + ex.toString) }
        ))
    }

    override def onHandlerNotFound(request: RequestHeader) = {
        Future.successful(NotFound(
            views.html.main("DataAPI Error: ") {new Html("Handler not found: " + request.path) }
        ))
    }

    override def onBadRequest(request: RequestHeader, error: String) = {
        Future.successful(BadRequest("Bad Request: " + error))
    }

}