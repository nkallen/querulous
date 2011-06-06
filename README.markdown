# Querulous

An agreeable way to talk to your database.

## License

Copyright 2010 Twitter, Inc. See included LICENSE file.

## Features

* Handles all the JDBC bullshit so you don't have to: type casting for primitives and collections, exception handling and transactions, and so forth;
* Fault tolerant: configurable strategies such as timeouts, mark-dead thresholds, and retries;
* Designed for operability: rich statistics about your database usage and extensive debug logging;
* Minimalist: minimal code, minimal assumptions, minimal dependencies. You write highly-tuned SQL and we get out of the way;
* Highly modular, highly configurable.

The Querulous source repository is [available on Github](http://github.com/twitter/querulous/). Patches and contributions are
welcome.

## Understanding the Implementation

`Querulous` is made out of three components: QueryEvaluators, Queries, and Databases.

* QueryEvaluators are a convenient procedural interface for executing queries.
* Queries are objects representing a SELECT/UPDATE/INSERT/DELETE SQL Query. They are responsible for most type-casting, timeouts, and so forth. You will rarely interact with Queries directly.
* Databases reserve and release connections an actual database.

Each of these three kinds of objects implement an interface. Enhanced functionality is meant to be "layered-on" by wrapping decorators around these objects that implement the enhanced functionality and delegate the primitive functionality.

Each of the three components are meant to be instantiated with their corresponding factories (e.g., QueryEvaluatorFactory, DatabaseFactory, etc.). The system is made configurable by constructing factories that manufacture the Decorators you're interested in. For example,

    val queryFactory = new DebuggingQueryFactory(new TimingOutQueryFactory(new SqlQueryFactory))
    val query = queryFactory(...) // this query will have debugging information and timeouts!

## Usage

### Basic Usage

Create a QueryEvaluator by connecting to a database host with a username and password:

    import com.twitter.querulous.evaluator.QueryEvaluator
    val queryEvaluator = QueryEvaluator("host", "username", "password")

Run a query or two:

    val users = queryEvaluator.select("SELECT * FROM users WHERE id IN (?) OR name = ?", List(1,2,3), "Jacques") { row =>
      new User(row.getInt("id"), row.getString("name"))
    }
    queryEvaluator.execute("INSERT INTO users VALUES (?, ?)", 1, "Jacques")

Note that sequences are handled automatically (i.e., you only need one question-mark (?)).

Run a query in a transaction for enhanced pleasure:

    queryEvaluator.transaction { transaction =>
      transaction.select("SELECT ... FOR UPDATE", ...)
      transaction.execute("INSERT INTO users VALUES (?, ?)", 1, "Jacques")
      transaction.execute("INSERT INTO users VALUES (?, ?)", 2, "Luc")
    }

The yielded transaction object implements the same interface as QueryEvaluator. Note that the transaction will be rolled back if you raise an exception.

### Advanced Usage

For production-quality use of `Querulous` you'll want to set configuration options and layer-on more functionality. Here is the maximally configurable, if somewhat elaborate, way to instantiate a QueryEvaluator

    import com.twitter.querulous.evaluator._
    import com.twitter.querulous.query._
    import com.twitter.querulous.database._

    val queryFactory = new SqlQueryFactory
    val apachePoolingDatabaseFactory = new apachePoolingDatabaseFactory(
      minOpenConnections:                 Int,      // minimum number of open/active connections at all times
      maxOpenConnections:                 Int,      // minimum number of open/active connections at all times
      checkConnectionHealthWhenIdleFor:   Duration, // asynchronously check the health of open connections every `checkConnectionHealthWhenIdleFor` amount of time
      maxWaitForConnectionReservation:    Duration, // maximum amount of time you're willing to wait to reserve a connection from the pool; throw an exception otherwise
      checkConnectionHealthOnReservation: Boolean,  // check connection health when reserving the connection from the pool
      evictConnectionIfIdleFor:           Duration  // destroy connections if they are idle for longer than `evictConnectionIfIdleFor` amount of time
    )
    val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(apachePoolingDatabaseFactory, queryFactory)
    val queryEvaluator = queryEvaluatorFactory(List("primaryhost", "fallbackhost1", "fallbackhost2", ...), "username", "password")

Now comes the fun part.

#### Query Decorators

Suppose you want timeouts around queries:

    val queryFactory = new TimingOutQueryFactory(new SqlQueryFactory, 3.seconds)

Suppose you ALSO want to retry queries up to 5 times:

    val queryFactory = new RetryingQueryFactory(new TimingOutQueryFactory(new SqlQueryFactory, 3000.millis), 5)

Suppose you have no idea what's going on and need some debug info:

    val queryFactory = new DebuggingQueryFactory(new RetryingQueryFactory(new TimingOutQueryFactory(new SqlQueryFactory, 3.seconds), 5), println)

You'll notice, at this point, that all of these advanced features can be layered-on by composing QueryFactories. In what follows, I'll omit some layering to keep the examples terse.

Suppose you want to measure average and standard deviation of latency, and query counts:

    val stats = new StatsCollector
    val queryFactory = new StatsCollectingQueryFactory(new SqlQueryFactory, stats)

See the section [Statistics Collection] for more information.

#### Database Decorators

Suppose you want to measure latency around the reserve/release operations of the Database:

    val stats = new StatsCollector
    val databaseFactory = new StatsCollectingDatabase(new ApachePoolingDatabaseFactory(...), stats)

Suppose you are actually dynamically connecting to dozens of hosts (because of a sharding strategy or something similar) and you want to maintain proper connection limits. You can memoize your database connections like this:

    val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(...))

#### QueryEvaluator Decorators

Suppose you want to automatically disable all connections to a particular host after a certain number of SQL Exceptions (timeouts, etc.):

    val queryEvaluatorFactory = new AutoDisablingQueryEvaluatorFactory(new StandardQueryEvaluatorFactory(databaseFactory, queryFactory))

### Async API

Querulous also contains an async API based on
[`com.twitter.util.Future`](http://github.com/twitter/util). The trait
`AsyncQueryEvaluator` mirrors `QueryEvaluator` in terms of
functionality, the key difference being that methods immediately
return values wrapped in a `Future`. Internally, blocking JDBC calls
are executed within a thread pool.

    // returns Future[Seq[User]]
    val future = queryEvaluator.select("SELECT * FROM users WHERE id IN (?) OR name = ?", List(1,2,3), "Jacques") { row =>
      new User(row.getInt("id"), row.getString("name"))
    }

    // Futures support a functional, monadic interface:
    val tweetsFuture = future flatMap { users =>
      queryEvaluator.select("SELECT * FROM tweets WHERE user_id IN (?)", users.map(_.id)) { row =>
        new Tweet(row.getInt("id"), row.getString("text"))
      }
    }

    // futures only block when unwrapped.
    val tweets = tweetsFuture.apply()

See [the Future API reference](http://twitter.github.com/util/util-core/target/site/doc/main/api/com/twitter/util/Future.html)
for more information.

### Recommended Configuration Options

* Set minActive equal to maxActive. This ensures that the system is fully utilizing the connection resource even when the system is idle. This is good because you will not be surprised by connection usage (and e.g., unexpectedly hit server-side connection limits) during peak load.
* Set minActive equal to maxActive equal to the MySql connection limit divided by the number of instances of your client process
* Set testIdle to 1.second or so. It should be substantially less than the server-side connection timeout.
* Set maxWait to 10.millis--to start. In general, it should be set to the average experienced latency plus twice the standard deviation. Gather statistics!
* Set minEvictableIdle to 5.minutes or more. It has no effect when minActive equals maxActive, but in case these differ you don't want excessive connection churning. It should certainly be less than or equal to the server-side connection timeout.


## Statistics Collection

StatsCollector is actually just a trait that you'll need to implement using your favorite statistics collecting library. My favorite is [Ostrich](http://github.com/robey/ostrich) and you can write an adapter in a few lines of code. Here is one such adapter:

    val stats = new StatsCollector {
      def incr(name: String, count: Int) = Stats.incr(name, count)
      def time[A](name: String)(f: => A): A = Stats.time(name)(f)
    }
    val databaseFactory = new StatsCollectingDatabaseFactory(new ApachePoolingDatabaseFactory(...), stats)

## Configuration Traits

Querulous comes with a set of configuration/builder traits, designed
to be used with
[com.twitter.util.Eval](https://github.com/twitter/util) or in code:

    import com.twitter.querulous.config._
    import com.twitter.conversions.time._

    val config = com.twitter.querulous.config.QueryEvaluator {
      lazy val log = Logger.get()

      autoDisable = new AutoDisablingQueryEvaluator {
        val errorCount = 100
        val interval   = 60.seconds
      }

      database.memoize = true

      database.autoDisable = new AutoDisablingDatabase {
        val errorCount = 200
        val interval   = 60.seconds
      }

      database.pool = new ApachePoolingDatabase {
        sizeMin          = 24
        sizeMax          = 24
        maxWait          = 5.seconds
        minEvictableIdle = 60.seconds
        testIdle         = 1.second
        testOnBorrow     = false
      }

      database.timeout = new TimingOutDatabase {
        poolSize  = 10
        queueSize = 10000
        open      = 100.millis
      }

      query.debug   = { s => log.ifDebug(s) }
      query.retries = tcpLevelRetries

      query.timeouts = Map(
        QueryClass.Select  -> QueryTimeout(individualFilterTimeout),
        QueryClass.Execute -> QueryTimeout(individualFilterTimeout)
      )
    }

    val queryEvaluatorFactory = config()
    val queryEvaluator = queryEvaluatorFactory()


## Installation

### Maven

Add the following dependency and repository stanzas to your project's configuration

    <dependency>
        <groupId>com.twitter</groupId>
        <artifactId>querulous</artifactId>
        <version>1.1.0</version>
    </dependency>

    <repository>
      <id>twitter.com</id>
      <url>http://maven.twttr.com/</url>
    </repository>

### Ivy

Add the following dependency to ivy.xml

    <dependency org="com.twitter" name="querulous" rev="1.1.0"/>

and the following repository to ivysettings.xml

    <ibiblio name="twitter.com" m2compatible="true" root="http://maven.twttr.com/" />

## Running Tests

Most of the tests are unit tests and are heavily mocked. However, some
tests run database queries. You should set the environment variables
`DB_USERNAME` and `DB_PASSWORD` to something that actually works for
your system. Then, from the command line, simply run:

    % sbt test

## Reporting problems

If you run into any trouble or find bugs, please report them via [the Github issue tracker](http://github.com/nkallen/querulous/issues).

## Contributors

* Nick Kallen
* Robey Pointer
* Ed Ceaser
* Utkarsh Srivastava
* Matt Freels
