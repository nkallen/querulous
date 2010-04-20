import sbt._
import java.io.File

class QuerulousProject(info: ProjectInfo) extends DefaultProject(info) {
  override def disableCrossPaths = true

  override def ivyCacheDirectory = Some(Path.fromFile(new File(System.getProperty("user.home"))) / ".ivy2-sbt" ##)

  val jbossRepository   = "jboss" at "http://repository.jboss.org/maven2/"
  val lagRepository     = "lag.net" at "http://www.lag.net/repo/"
  val twitterRepository = "twitter.com" at "http://www.lag.net/nest/"

  val asm       = "asm" % "asm" %  "1.5.3"
  val cglib     = "cglib" % "cglib" % "2.1_3"
  val configgy  = "net.lag" % "configgy" % "1.4.3"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.2.2"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
  val pool      = "commons-pool" % "commons-pool" % "1.3"
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.1"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
}
