import javax.inject._

import play.api._
import play.api.http.HttpFilters
import play.api.mvc._
import play.filters.cors
import play.api.http.DefaultHttpFilters
import play.filters.cors.CORSFilter
import play.mvc.EssentialFilter

//import filters.ExampleFilter

/**
 * This class configures filters that run on every request. This
 * class is queried by Play to get a list of filters.
 * @param env Basic environment settings for the current application.
 * @param corsFilter A filter that handles CORS for each request.
 */

@Singleton
class Filters @Inject() (env: Environment, corsFilter: CORSFilter) extends DefaultHttpFilters(corsFilter)
