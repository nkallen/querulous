import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twitter = "twitter.com" at "http://maven.twttr.com/"
  val standardProject = "com.twitter" % "standard-project" % "0.12.7"
}
