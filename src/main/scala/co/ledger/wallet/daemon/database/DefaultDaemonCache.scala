package co.ledger.wallet.daemon.database

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.exceptions._
import co.ledger.wallet.daemon.models.Account._
import co.ledger.wallet.daemon.models.Operations.PackedOperationsView
import co.ledger.wallet.daemon.models._
import co.ledger.wallet.daemon.schedulers.observers.{NewOperationEventReceiver, SynchronizationResult}
import co.ledger.wallet.daemon.services.LogMsgMaker
import com.twitter.inject.Logging
import javax.inject.Singleton
import slick.jdbc.JdbcBackend.Database

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DefaultDaemonCache() extends DaemonCache with Logging {

  def dbMigration: Future[Unit] = {
    dbDao.migrate()
  }

  def syncOperations(implicit ec: ExecutionContext): Future[Seq[SynchronizationResult]] = {
    Future.sequence(poolCache.values.map { pool =>
      pool.sync()
    }).map(_.flatten.toSeq)
  }

  def getPreviousBatchAccountOperations(previous: UUID,
                                        fullOp: Int, accountInfo: AccountInfo)(implicit ec: ExecutionContext): Future[PackedOperationsView] = {
    withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        val previousRecord = opsCache.getPreviousOperationRecord(previous)
        for {
          ops <- account.operations(previousRecord.offset(), previousRecord.batch, fullOp)
          opsView <- Future.sequence(ops.map { op => Operations.getView(op, wallet, account) })
        } yield PackedOperationsView(previousRecord.previous, previousRecord.next, opsView)
    }
  }

  def getNextBatchAccountOperations(next: UUID,
                                    fullOp: Int, accountInfo: AccountInfo)(implicit ec: ExecutionContext): Future[PackedOperationsView] = {
    withAccountAndWalletAndPool(accountInfo) {
      case (account, wallet, pool) =>
        val candidate = opsCache.getOperationCandidate(next)
        for {
          ops <- account.operations(candidate.offset(), candidate.batch, fullOp)
          realBatch = if (ops.size < candidate.batch) ops.size else candidate.batch
          next = if (realBatch < candidate.batch) None else candidate.next
          previous = candidate.previous
          operationRecord = opsCache.insertOperation(candidate.id, pool.name, accountInfo.walletName, accountInfo.accountIndex, candidate.offset(), candidate.batch, next, previous)
          opsView <- Future.sequence(ops.map { op => Operations.getView(op, wallet, account) })
        } yield PackedOperationsView(operationRecord.previous, operationRecord.next, opsView)
    }
  }

  def getAccountOperations(batch: Int, fullOp: Int, accountInfo: AccountInfo)(implicit ec: ExecutionContext): Future[PackedOperationsView] = {
    withAccountAndWalletAndPool(accountInfo) {
      case (account, wallet, pool) =>
        val offset = 0
        for {
          ops <- account.operations(offset, batch, fullOp)
          realBatch = if (ops.size < batch) ops.size else batch
          next = if (realBatch < batch) None else Option(UUID.randomUUID())
          previous = None
          operationRecord = opsCache.insertOperation(UUID.randomUUID(), pool.name, accountInfo.walletName, accountInfo.accountIndex, offset, batch, next, previous)
          opsView <- Future.sequence(ops.map { op => Operations.getView(op, wallet, account) })
        } yield PackedOperationsView(operationRecord.previous, operationRecord.next, opsView)
    }
  }

  private[database] val dbDao = new DatabaseDao(Database.forConfig(DaemonConfiguration.dbProfileName))
  private[database] val opsCache: OperationCache = new OperationCache()
  private val poolCache: concurrent.Map[String, Pool] = new ConcurrentHashMap[String, Pool]().asScala

  /**
   * Delete pool will:
   *  1. remove the pool from daemon database
   *  2. unsubscribe event receivers to core library, see details on method `clear` from `Pool`
   *  3. remove the operations were done on this pool, which includes all underlying wallets and accounts
   *  4. remove the pool from cache.
   *
   * @param name the name of wallet pool needs to be deleted.
   * @return a Future of Unit.
   */
  def deletePool(name: String)(implicit ec: ExecutionContext): Future[Unit] = {
    dbDao.deletePool(name).flatMap { deletedPool =>
      if (deletedPool > 0) opsCache.deleteOperations(name)
      clearCache(name).map { _ =>
        info(LogMsgMaker.newInstance("Pool deleted").append("name", name).toString())
      }
    }
  }

  private def clearCache(poolName: String): Future[Unit] = poolCache.remove(poolName) match {
    case Some(p) => p.clear
    case None => Future.unit
  }

  def addPoolIfNotExit(name: String)(implicit ec: ExecutionContext): Future[Pool] = {
    val dto = PoolDto(name)
    dbDao.insertPool(dto).flatMap { _ =>
      toCacheAndStartListen(dto).map { pool =>
        info(LogMsgMaker.newInstance("Pool created").append("name", name).toString())
        pool
      }
    }.recoverWith {
      case _: WalletPoolAlreadyExistException =>
        warn(LogMsgMaker.newInstance("Pool already exist").append("name", name).toString())
        pool(name).map(op => op.getOrElse(throw WalletPoolNotFoundException(s"Failed to retrieve pool, inconsistent state for pool $name")))

    }
  }

  /**
   * Getter for individual pool with specified name. This method will perform a daemon database search in order
   * to get the most up to date information. If specified pool doesn't exist in database but in cache. The cached
   * pool will be cleared. See `clear` method from Pool for detailed actions.
   *
   * @param name the name of wallet pool.
   * @return a Future of `co.ledger.wallet.daemon.models.Pool` instance Option.
   */
  def pool(name: String)(implicit ec: ExecutionContext): Future[Option[Pool]] = {
    dbDao.getPool(name).flatMap {
      case Some(p) => toCacheAndStartListen(p).map(Option(_))
      case None => clearCache(name).map { _ => None }
    }
  }

  private def toCacheAndStartListen(p: PoolDto)(implicit ec: ExecutionContext): Future[Pool] = {
    poolCache.get(p.name) match {
      case Some(pool) => Future.successful(pool)
      case None => Pool.newCoreInstance(p.name).flatMap { coreP =>
        poolCache.put(p.name, Pool.newInstance(coreP))
        debug(s"Add ${poolCache(p.name)} to cache")
        poolCache(p.name).registerEventReceiver(new NewOperationEventReceiver(p.name, opsCache))
        poolCache(p.name).startRealTimeObserver().map { _ => poolCache(p.name) }
      }
    }
  }

  /**
   * Obtain available pools of this user. The method performs database call(s), adds the missing
   * pools to cache.
   *
   * @return the resulting pools. The result may contain less entities
   *         than the cached entities.
   */
  def pools(implicit ec: ExecutionContext): Future[Seq[Pool]] = for {
    poolDtos <- dbDao.getPools
    pools <- Future.sequence(poolDtos.map { pool => toCacheAndStartListen(pool) })
  } yield pools

  override def createWalletPool(poolInfo: PoolInfo)(implicit ec: ExecutionContext): Future[Pool] = {
    addPoolIfNotExit(poolInfo.poolName)
  }

  override def getWalletPool(poolInfo: PoolInfo)(implicit ec: ExecutionContext): Future[Option[Pool]] = {
    Future.successful(poolCache.get(poolInfo.poolName))
  }
}
