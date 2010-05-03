import sbt._
import com.twitter.sbt.StandardProject


class QuerulousProject(info: ProjectInfo) extends StandardProject(info) {
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"
  val configgy  = "net.lag" % "configgy" % "1.5.2"
  val asm       = "asm" % "asm" %  "1.5.3"
  val cglib     = "cglib" % "cglib" % "2.1_3"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.2.2"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
  val pool      = "commons-pool" % "commons-pool" % "1.3"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
  val publishTo = Resolver.sftp("green.lag.net", "green.lag.net", "/web/repo")
}
