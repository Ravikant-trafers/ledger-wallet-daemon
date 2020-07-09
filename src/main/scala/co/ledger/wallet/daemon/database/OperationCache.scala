package co.ledger.wallet.daemon.database

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import co.ledger.wallet.daemon.exceptions.OperationNotFoundException
import co.ledger.wallet.daemon.services.LogMsgMaker
import com.twitter.inject.Logging
import javax.inject.Singleton
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import java.time.Duration

import com.google.common.cache.CacheBuilder

import scala.collection._
import co.ledger.wallet.daemon.configurations.DaemonConfiguration

@Singleton
class OperationCache extends Logging {

  type Cache[K, V] = concurrent.Map[K, V]

  private def newCache[K <: AnyRef, V <: AnyRef]: Cache[K, V] =
    CacheBuilder.newBuilder()
      .maximumSize(DaemonConfiguration.paginationTokenMaxSize)
      .expireAfterAccess(Duration.ofMinutes(DaemonConfiguration.paginationTokenTtlMin))
      .build[K, V]().asMap().asScala

  /**
    * Create and insert an operation query record.
    *
    * @param id the UUID of this operation query record.
    * @param poolName the identifier of pool in database.
    * @param walletName the name of wallet.
    * @param accountIndex the unique index of the account.
    * @param offset the offset to the last operation of the total operations.
    * @param batch the size of queried sequence operations.
    * @param next the UUID of operation query record to find the next operation query parameters.
    * @param previous the UUID of operation query record to find previous query.
    * @return a operation query record.
    */
  def insertOperation(
                       id: UUID,
                       poolName: String,
                       walletName: String,
                       accountIndex: Int,
                       offset: Int,
                       batch: Int,
                       next: Option[UUID],
                       previous: Option[UUID]): AtomicRecord = {
    if (cache.contains(id)) { cache(id) }
    else {
      val newRecord = new AtomicRecord(id, poolName, Option(walletName), Option(accountIndex), batch, new AtomicInteger(offset), next, previous)
      cache.put(id, newRecord)
      next.map { nexts.put(_, id)}
      poolTrees.get(poolName) match {
        case Some(poolTree) => poolTree.insertOperation(walletName, accountIndex, newRecord.id)
        case None => poolTrees.put(poolName, newPoolTreeInstance(poolName, walletName, accountIndex, newRecord.id))
      }
      newRecord
    }
  }

  /**
    * Getter for obtaining existing operation record as candidate with specified id or if not exist create a new record as candidate.
    *
    * The logic matches:
    *
    * None        |R1          |R2           |R3          |
    *             |            |R1.next      |R2.next     |R3.next
    * R1.previous |R2.previous |R3.previous  |            |
    *
    * If (id == R3) then R3 will be returned as candidate
    * If (id == R3.next) then a new record as the next of R3 will be returned as candidate
    *
    * @param id the UUID of query record candidate.
    * @return a operation query record.
    */
  def getOperationCandidate(id: UUID): AtomicRecord = {
    cache.getOrElse(id, nexts.get(id) match {
      case Some(current) => cache.get(current) match {
        case Some(record) => new AtomicRecord(
          id,
          record.poolName,
          record.walletName,
          record.accountIndex,
          record.batch,
          new AtomicInteger(record.batch + record.offset()),
          Some(UUID.randomUUID()),
          Some(current))
        case None => throw OperationNotFoundException(current)
      }
      case None => throw OperationNotFoundException(id)
    })
  }

  /**
    * Getter for obtaining record with specified id or if not exist the previous record of this id.
    *
    * The logic matches:
    *
    * None        |R1          |R2           |R3          |
    *             |            |R1.next      |R2.next     |R3.next
    * R1.previous |R2.previous |R3.previous  |            |
    *
    * If (id == R2.id) then R2 will be returned
    * If (id == R3.next) then R3 will be returned
    *
    * @param id the UUID of query record, the record should already exist.
    * @return the operation query record.
    */
  def getPreviousOperationRecord(id: UUID): AtomicRecord = {
    cache.getOrElse(id, nexts.get(id) match {
      case Some(pre) => cache.getOrElse(pre, throw OperationNotFoundException(id))
      case None => throw OperationNotFoundException(id)
    })
  }

  /**
    * Update offset of existing records with specified identifiers.
    *
    * @param poolName the pool identifier in database.
    * @param walletName the name of the wallet.
    * @param accountIndex the unique index of account.
    */
  def updateOffset(poolName: String, walletName: String, accountIndex: Int): Unit = {
    if (poolTrees.contains(poolName)) {
      poolTrees(poolName).operations(walletName, accountIndex).foreach { op =>
        val lastOffset = cache(op).incrementOffset()
        debug(LogMsgMaker.newInstance("Update offset")
          .append("to", lastOffset)
          .append("pool", poolName)
          .append("wallet", walletName)
          .append("account", accountIndex)
          .toString())
      }
    }
  }

  def deleteOperations(poolName: String): Unit = {
    poolTrees.remove(poolName).foreach { poolTree =>
      poolTree.operations.foreach { op =>
        // remove operation record from cache and nexts
        cache.remove(op).map { record => record.next.map { nextUUID => nexts.remove(nextUUID) } }
      }
    }
  }

  private def newPoolTreeInstance(poolName: String, wallet: String, account: Int, operation: UUID): PoolTree = {
    val wallets: Cache[String, WalletTree] = newCache
    wallets.put(wallet, newWalletTreeInstance(wallet, account, operation))
    new PoolTree(poolName, wallets)
  }
  private[this] val cache: Cache[UUID, AtomicRecord] = newCache[UUID, AtomicRecord]
  private[this] val nexts: Cache[UUID, UUID] = newCache[UUID, UUID]
  private[this] val poolTrees: Cache[String, PoolTree] = new ConcurrentHashMap[String, PoolTree].asScala

  class PoolTree(val poolName: String, val wallets: Cache[String, WalletTree]) {

    def insertOperation(wallet: String, account: Int, operation: UUID): Unit = wallets.get(wallet) match {
      case Some(tree) => tree.insertOperation(account, operation)
      case None => wallets.put(wallet, newWalletTreeInstance(wallet, account, operation))
    }

    def operations(wallet: String, account: Int): Set[UUID] = if (wallets.contains(wallet)) wallets(wallet).operations(account) else Set.empty[UUID]

    def operations: Set[UUID] = wallets.values.flatMap { wallet => wallet.operations }.toSet
  }

  def newWalletTreeInstance(walletName: String, accountIndex: Int, operation: UUID): WalletTree = {
    val accounts: Cache[java.lang.Integer, AccountTree] = newCache[java.lang.Integer, AccountTree]
    accounts.put(accountIndex, newAccountTreeInstance(accountIndex, operation))
    new WalletTree(walletName, accounts)
  }

  class WalletTree(val walletName: String, val accounts: Cache[java.lang.Integer, AccountTree]) {

    def insertOperation(account: Int, operation: UUID): Unit = accounts.get(account) match {
      case Some(tree) => tree.insertOperation(operation)
      case None => accounts.put(account, newAccountTreeInstance(account, operation))
    }

    def operations(account: Int): Set[UUID] = if (accounts.contains(account)) accounts(account).operations.keys.toSet else Set.empty[UUID]

    def operations: Set[UUID] = accounts.values.flatMap { account => account.operations.keys }.toSet
  }

  def newAccountTreeInstance(index: Int, operation: UUID): AccountTree = {
    new AccountTree(index, newCache[UUID, java.lang.Boolean] += (operation -> true))
  }

  class AccountTree(val index: Int, val operations: Cache[UUID, java.lang.Boolean]) {

    def containsOperation(operationId: UUID): Boolean = operations.contains(operationId)

    def insertOperation(operationId: UUID): operations.type = operations += (operationId -> true)
  }

  class AtomicRecord(val id: UUID,
                     val poolName: String,
                     val walletName: Option[String],
                     val accountIndex: Option[Int],
                     val batch: Int,
                     private val ofst: AtomicInteger,
                     val next: Option[UUID],
                     val previous: Option[UUID]) {

    /**
      * Method to increment the offset. This operation is thread safe.
      *
      * @return the incremented offset.
      */
    def incrementOffset(): Long = ofst.incrementAndGet()

    /**
      * Method to retrieve the offset.
      *
      * @return the offset.
      */
    def offset(): Int = ofst.get()
  }
}
