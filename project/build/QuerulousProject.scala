import sbt._
import Process._
import com.twitter.sbt._

class QuerulousProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher {
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"
  val configgy  = "net.lag" % "configgy" % "1.5.2"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.4"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val pool      = "commons-pool" % "commons-pool" % "1.5.4"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val jmock     = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib     = "cglib" % "cglib" % "2.1_3" % "test"
  val asm       = "asm" % "asm" %  "1.5.3" % "test"
}
