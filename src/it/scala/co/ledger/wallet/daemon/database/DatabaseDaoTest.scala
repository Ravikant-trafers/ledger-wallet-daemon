package co.ledger.wallet.daemon.database

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import com.twitter.inject.Logging
import org.junit.Assert._
import org.junit.{BeforeClass, Test}
import org.scalatest.junit.AssertionsForJUnit
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DatabaseDaoTest extends AssertionsForJUnit {
  import DatabaseDaoTest._

  @Test
  def createAndGetPool(): Unit = {
    Await.result(dbDao.insertPool(PoolDto("test_pool1")), Duration.Inf)
    val pool = Await.result(dbDao.getPool("test_pool1"), Duration.Inf)
    assertTrue(pool.isDefined)
  }

  @Test
  def allPools(): Unit = {
    Await.result(dbDao.insertPool(PoolDto("test_pool2")), Duration.Inf)
    val pool = Await.result(dbDao.getPools, Duration.Inf)
    assertTrue(pool.nonEmpty)
  }

  @Test
  def deletePool(): Unit = {
    Await.result(dbDao.insertPool(PoolDto("test_pool3")), Duration.Inf)
    Await.result(dbDao.deletePool("test_pool3"), Duration.Inf)
    val pool = Await.result(dbDao.getPool("test_pool3"), Duration.Inf)
    assertTrue(pool.isEmpty)
  }

}

object DatabaseDaoTest extends Logging {
  @BeforeClass def initialization(): Unit = {
    debug("******************************* before class start")
    Await.result(dbDao.migrate(), Duration.Inf)
    debug("******************************* before class end")
  }
  private val dbDao = new DatabaseDao(Database.forConfig(DaemonConfiguration.dbProfileName))
}