// Schema [description: string, end_time: string, id: string, name: string, obj_type: string, period: string, stats_type: string, sys_time: string, title: string, value: string]

import org.joda.time.{DateTime, Period}
import org.apache.spark.sql.hive.HiveContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

implicit val formats = DefaultFormats

val hiveCtx = new HiveContext(sc)

val dateNow = new DateTime().minusDays(1)
val dateTarget = dateNow.minusDays(0)
val dateDayAgo = dateTarget.minusDays(1)
val arrNow:Array[String] = dateNow.toLocalDate.toString.split("-")
val arrNew:Array[String] = dateTarget.toLocalDate.toString.split("-")
val arrOld:Array[String] = dateDayAgo.toLocalDate.toString.split("-")

val pageId = "1495821923975356"
val s3PathBucket = "s3n://gps-stats"
val s3PathKeyPrefix = "fb-graph-"
val s3PathKeySuffix = ".post_insights.data.test"

val s3PathYmdNew = arrNew(0) + "/" + arrNew(1) + "/" + arrNew(2)
val s3PathNew = s3PathBucket + "/" + s3PathYmdNew  + "/" + s3PathKeyPrefix + pageId + "-" + dateTarget.toLocalDate + s3PathKeySuffix
val s3PathYmdOld = arrOld(0) + "/" + arrOld(1) + "/" + arrOld(2)
val s3PathOld = s3PathBucket + "/" + s3PathYmdOld  + "/" + s3PathKeyPrefix + pageId + "-" + dateDayAgo.toLocalDate + s3PathKeySuffix

val s3PathYmdNow = arrNow(0) + "/" + arrNow(1) + "/" + arrNow(2)
val s3OutputPath = s3PathBucket + "/" + s3PathYmdNow  + "/"

val statsNew = hiveCtx.jsonFile(s3PathNew)
val statsOld = hiveCtx.jsonFile(s3PathOld)

val joined = statsNew.join(statsOld, statsNew("id") === statsOld("id"), "left_outer")

val diffed = joined.select(statsNew("description"), statsNew("title"), statsNew("name"), statsNew("id"), statsNew("obj_type"), statsNew("stats_type"), statsNew("plat_id"), statsNew("proj_id_plat"), statsNew("asset_id_plat"), statsNew("sys_time"), statsOld("value").as('old_value), statsNew("value").as('new_value))

var modified  = diffed.map( x => {

    import DataCaseClasses.{Action, Reaction, Stats}

    def isAllDigits(x: String) = x forall Character.isDigit

    val col0=x.getAs[String]("description").replace("Lifetime:", "DailyComp");
    var colDiff:String = "0"
    var oldVal = x.getAs[String]("old_value")
    val newVal = x.getAs[String]("new_value")

    if (oldVal == null) {

        colDiff = newVal

    } else if (oldVal.contains("haha")) {

	val patReaction = """\{("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+)\}""".r
	var reactOld:Reaction = null
	var reactNew:Reaction = null

        try {
	    oldVal match {
	    	case patReaction(love, loveVal, haha, hahaVal, like, likeVal, sorry, sorryVal, anger, angerVal, wow, wowVal) =>
        		reactOld = Reaction(loveVal.toInt, hahaVal.toInt, likeVal.toInt, sorryVal.toInt, angerVal.toInt, wowVal.toInt)
    		case _ => println("Nothing")
	    }
            newVal match {
                case patReaction(love, loveVal, haha, hahaVal, like, likeVal, sorry, sorryVal, anger, angerVal, wow, wowVal) =>
                        reactNew = Reaction(loveVal.toInt, hahaVal.toInt, likeVal.toInt, sorryVal.toInt, angerVal.toInt, wowVal.toInt)
                case _ => println("Nothing")
            }   
	    colDiff = (reactNew - reactOld).toString
        } catch {
            case e: Exception => println("Exception in if haha: " + e.getMessage)
        }

    } else if (oldVal.contains("share") || oldVal.contains("comment")) {

        val patAction = """\{("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+)\}""".r
        val patActionShort = """\{("[a-z]+"):([0-9]+),("[a-z]+"):([0-9]+)\}""".r
        var actOld:Action = null
        var actNew:Action = null

        try {
            oldVal match {
                case patAction(like, likeVal, share, shareVal, comment, commentVal) =>
                        actOld = Action(likeVal.toInt, shareVal.toInt, commentVal.toInt)
		case patActionShort(like, likeVal, socName, socVal) =>
			actOld = Action(likeVal.toInt, socVal.toInt, 0)
                case _ => println("Nothing")
            }
            newVal match {
                case patAction(like, likeVal, share, shareVal, comment, commentVal) =>
                        actNew = Action(likeVal.toInt, shareVal.toInt, commentVal.toInt)
                case patActionShort(like, likeVal, socName, socVal) =>
                        actNew = Action(likeVal.toInt, socVal.toInt, 0)
                case _ => println("Nothing")
            } 
            colDiff = (actNew - actOld).toString
        } catch {
            case e: Exception => println("Exception in if share: " + e.getMessage)
        }

    } else if (isAllDigits(newVal) && isAllDigits(oldVal)) {

        try {
            colDiff = (newVal.toLong - oldVal.toLong ).toString
        } catch {
            case e: Exception => println("Exception in else part:" + e.getMessage)
        }

    } else {
	colDiff = "0"
    }

    Stats( col0, x.getAs[String]("title").replace("Lifetime", "Daily"), x.getAs[String]("name"), x.getAs[String]("id"), x.getAs[String]("plat_id"), x.getAs[String]("proj_id_plat"), x.getAs[String]("asset_id_plat"), x.getAs[String]("obj_type"), "day", "DailyDiff", x.getAs[String]("sys_time"), x.getAs[String]("old_value"), x.getAs[String]("new_value"), colDiff)

} )

modified.saveAsTextFile(s3OutputPath + "video_insights_diff")

//val finalRes = modified.toDF()
//finalRes.show
//finalRes.repartition(1).save(s3OutputPath + "video_insights_diff", "json")

//System.exit(0)
