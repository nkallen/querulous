import sbt._
import Process._
import com.twitter.sbt._

class QuerulousProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher {
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val specs     = "org.scala-tools.testing" %% "specs" % "1.6.5"
  val configgy = "net.lag" % "configgy" % "2.0.0"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.4"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.13"
  val pool      = "commons-pool" % "commons-pool" % "1.5.4"
  val xrayspecs = "com.twitter" %% "xrayspecs" % "2.0"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val jmock     = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib     = "cglib" % "cglib" % "2.1_3" % "test"
  val asm       = "asm" % "asm" %  "1.5.3" % "test"

  override def disableCrossPaths = false

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
