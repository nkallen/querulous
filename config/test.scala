import com.twitter.querulous.config.Connection

new Connection {
  val hostnames = Seq("localhost")
  val database = "db_test"
  val username = {
    val userEnv = System.getenv("DB_USERNAME")
    if (userEnv == null) "root" else userEnv
  }

  val password = {
    val passEnv = System.getenv("DB_PASSWORD")
    if (passEnv == null) "" else passEnv
  }
}
