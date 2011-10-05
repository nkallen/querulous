import sbt._
import com.twitter.sbt._

class QuerulousProject(info: ProjectInfo) extends StandardParentProject(info)
with DefaultRepos
with SubversionPublisher {

  val coreProject = project(
    "querulous-core", "querulous-core",
    new CoreProject(_))

  /**
   * finagle compatible tracing for database requests
   */
  val tracingProject = project(
    "querulous-tracing", "querulous-tracing",
    new TracingProject(_), coreProject)

  trait Defaults
    extends ProjectDependencies
    with DefaultRepos
    with SubversionPublisher

  class CoreProject(info: ProjectInfo) extends StandardLibraryProject(info)
    with Defaults
  {
    val utilVersion = "1.11.8"

    val utilCore  = "com.twitter"  % "util-core"            % utilVersion
    val dbcp      = "commons-dbcp" % "commons-dbcp"         % "1.4"
    val mysqljdbc = "mysql"        % "mysql-connector-java" % "5.1.13"
    val pool      = "commons-pool" % "commons-pool"         % "1.5.4"

    val utilEval   = "com.twitter"             % "util-eval"          % utilVersion % "test"
    val scalaTools = "org.scala-lang"          % "scala-compiler"     % "2.8.1"     % "test"
    val hamcrest   = "org.hamcrest"            % "hamcrest-all"       % "1.1"       % "test"
    val specs      = "org.scala-tools.testing" % "specs_2.8.0"        % "1.6.5"     % "test"
    val objenesis  = "org.objenesis"           % "objenesis"          % "1.1"       % "test"
    val jmock      = "org.jmock"               % "jmock"              % "2.4.0"     % "test"
    val cglib      = "cglib"                   % "cglib"              % "2.2"       % "test"
    val asm        = "asm"                     % "asm"                % "1.5.3"     % "test"
    val dbcpTests  = "commons-dbcp"            % "commons-dbcp-tests" % "1.4"       % "test"
  }

  class TracingProject(info: ProjectInfo) extends StandardLibraryProject(info)
    with Defaults
  {
    val finagle   = "com.twitter"  % "finagle-core"         % "1.9.2"
  }

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
