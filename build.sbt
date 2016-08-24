
val slick = "com.typesafe.slick" %% "slick" % "2.1.0"
val slf4j = "org.slf4j" % "slf4j-nop" % "1.6.4"
val h2 = "com.h2database" % "h2" % "1.3.174"
val redshift = "com.amazon" % "redshift.jdbc41" % "1.1.13.1013"
val testplus = "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test

lazy val commonSettings = Seq(
	organization := "com.fresno",
	version := "2.0-SNAPSHOT",
	scalaVersion := "2.11.7"
)

lazy val root = (project in file("."))
		.enablePlugins(PlayScala)
    .settings(commonSettings: _*)
		.settings(
	   	 name := "FresnoDataApi"
		)

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  testplus
)

libraryDependencies ++= Seq(slick, slf4j, h2, redshift, filters)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/maven-releases/"
resolvers += "Typesafe Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "slick repo" at "https://mvnrepository.com/artifact/com.typesafe.play/play-slick_2.11"
resolvers += "mvnrepository.com" at "https://mvnrepository.com/artifact"


