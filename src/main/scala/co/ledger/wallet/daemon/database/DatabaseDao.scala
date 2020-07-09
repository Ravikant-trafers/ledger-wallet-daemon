package co.ledger.wallet.daemon.database

import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.Executors

import co.ledger.wallet.daemon.database.DBMigrations.Migrations
import co.ledger.wallet.daemon.exceptions._
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DatabaseDao @Inject()(db: Database) extends Logging {
  import Tables._
  import Tables.profile.api._
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(
    (r: Runnable) => new Thread(r, "database-access")
  ))

  def migrate(): Future[Unit] = {
    info("Start database migration")
    val lastMigrationVersion = databaseVersions.sortBy(_.version.desc).map(_.version).take(1).result.head
    db.run(lastMigrationVersion.transactionally) recover {
      case _ => -1
    } flatMap { currentVersion => {
        info(s"Current database version at $currentVersion")
        val maxVersion = Migrations.keys.toArray.sortWith(_ > _).head

        def migrate(version: Int, maxVersion: Int): Future[Unit] = {
          if (version > maxVersion) {
            info(s"Database version up to date at $maxVersion")
            Future.unit
          } else {
            info(s"Migrating version $version / $maxVersion")
            val rollbackMigrate = DBIO.seq(Migrations(version), insertDatabaseVersion(version))
            db.run(rollbackMigrate.transactionally).flatMap { _ =>
              info(s"version $version / $maxVersion migration done")
              migrate(version + 1, maxVersion)
            }
          }
        }

        migrate(currentVersion + 1, maxVersion)
      }
    }
  }

  def deletePool(poolName: String): Future[Int] = {
    safeRun(pools.filter(_.name === poolName).delete)
  }

  def getPools: Future[Seq[PoolDto]] =
    safeRun(pools.result).map(_.map(r => PoolDto(r.name)))

  def getPool(name: String): Future[Option[PoolDto]] =
    safeRun(pools.filter(_.name === name).result.headOption).map(_.map(r => PoolDto(r.name)))

  def insertPool(newPool: PoolDto): Future[Int] = {
    safeRun(pools.insertOrUpdate(PoolRow(newPool.name)))
  }

  private def safeRun[R](query: DBIO[R]): Future[R] =
    db.run(query.transactionally).recoverWith {
      case e: DaemonException => Future.failed(e)
      case others: Throwable => Future.failed(DaemonDatabaseException("Failed to run database query", others))
    }

  private def insertDatabaseVersion(version: Int): DBIO[Int] =
    databaseVersions += (version, new Timestamp(new Date().getTime))

}

case class PoolDto(name: String)
