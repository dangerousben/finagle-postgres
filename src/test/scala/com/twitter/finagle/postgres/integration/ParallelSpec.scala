package com.twitter.finagle
package postgres
package integration

import com.twitter.util.{ Await, Future }
import scala.util.Random

class ParallelSpec extends Spec {
  for {
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
    sslHost = sys.env.get("PG_SSL_HOST")
  } yield {

    val client = Postgres.Client()
      .withCredentials(user, password)
      .database(dbname)
      .withSessionPool.maxSize(16)
      .conditionally(useSsl, c => sslHost.fold(c.withTransport.tls)(c.withTransport.tls(_)))
      .newRichClient(hostPort)

    "PostgresClient" should {
      "safely run many prepared statements in parallel" in {
        val range = 0 until 128

        val results = Await.result(Future.collect(range.map(n =>
          client.prepareAndQuery(
            s"select $$1 n from (select pg_sleep_for('${Random.nextInt(1000)} milliseconds')) s", n,
          )(_.get[Int]("n"))
        ))).flatten

        results must contain theSameElementsAs range
      }
    }
  }
}
