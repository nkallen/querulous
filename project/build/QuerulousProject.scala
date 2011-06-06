import sbt._
import Process._
import com.twitter.sbt._

class QuerulousProject(info: ProjectInfo) extends StandardLibraryProject(info)
with DefaultRepos
with SubversionPublisher {

  val utilCore  = "com.twitter"  % "util-core"            % "1.8.13"
  val dbcp      = "commons-dbcp" % "commons-dbcp"         % "1.4"
  val mysqljdbc = "mysql"        % "mysql-connector-java" % "5.1.13"
  val pool      = "commons-pool" % "commons-pool"         % "1.5.4"

  val utilEval   = "com.twitter"             % "util-eval"      % "1.8.13" % "test"
  val scalaTools = "org.scala-lang"          % "scala-compiler" % "2.8.1"  % "test"
  val hamcrest   = "org.hamcrest"            % "hamcrest-all"   % "1.1"    % "test"
  val specs      = "org.scala-tools.testing" % "specs_2.8.0"    % "1.6.5"  % "test"
  val objenesis  = "org.objenesis"           % "objenesis"      % "1.1"    % "test"
  val jmock      = "org.jmock"               % "jmock"          % "2.4.0"  % "test"
  val cglib      = "cglib"                   % "cglib"          % "2.2"    % "test"
  val asm        = "asm"                     % "asm"            %  "1.5.3" % "test"

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
