import sbt._
import Process._
import com.twitter.sbt._

class QuerulousProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with InlineDependencies {
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val specs     = buildScalaVersion match {
    case "2.7.7" => "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
    case _ => "org.scala-tools.testing" %% "specs" % "1.6.5" % "test"
  }
  buildScalaVersion match {
    case "2.7.7" => inline("net.lag" % "configgy" % "1.6.6")
    case _ => inline("net.lag" % "configgy" % "2.0.0")
  }
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.4"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.13"
  val pool      = "commons-pool" % "commons-pool" % "1.5.4"
  val util      = "com.twitter" % "util" % "1.1.1"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val jmock     = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib     = "cglib" % "cglib" % "2.1_3" % "test"
  val asm       = "asm" % "asm" %  "1.5.3" % "test"

  override def disableCrossPaths = false

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
