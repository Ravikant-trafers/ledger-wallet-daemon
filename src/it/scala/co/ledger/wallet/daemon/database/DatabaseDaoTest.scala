package co.ledger.wallet.daemon.database

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.exceptions.{DaemonDatabaseException, UserAlreadyExistException}
import co.ledger.wallet.daemon.utils.HexUtils
import com.twitter.inject.Logging
import org.junit.Assert._
import org.junit.{BeforeClass, Test}
import org.scalatest.junit.AssertionsForJUnit
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DatabaseDaoTest extends AssertionsForJUnit {
  import DatabaseDaoTest._

  @Test def verifyDeletePoolNotDeletingUserWithNoPool(): Unit = {

    val pubKey = "33B4A94D8E33308DD08A3A8C937822101E229D85A2C0DFABC236A8C6A82E58076D"
    Await.result(dbDao.insertUser(UserDto(pubKey, 0)), Duration.Inf)
    val insertedUser = Await.result(dbDao.getUser(HexUtils.valueOf(pubKey)), Duration.Inf)
    val poolId = Await.result(dbDao.insertPool(PoolDto("myPool", insertedUser.get.id.get, "")), Duration.Inf)
    val existingPools = Await.result(dbDao.getPools(insertedUser.get.id.get), Duration.Inf)
    assertEquals(1, existingPools.size)
    assertEquals(poolId, existingPools.head.id.get)
    assertEquals("myPool", existingPools.head.name)
    val deletedPool = Await.result(dbDao.deletePool("myPool", insertedUser.get.id.get), Duration.Inf)
    assertEquals(deletedPool, existingPools.headOption)
    val leftoverUser = Await.result(dbDao.getUser(HexUtils.valueOf(pubKey)), Duration.Inf)
    assertFalse("User should not be deleted", leftoverUser.isEmpty)
    assertEquals(0, Await.result(dbDao.getPools(insertedUser.get.id.get), Duration.Inf).size)
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