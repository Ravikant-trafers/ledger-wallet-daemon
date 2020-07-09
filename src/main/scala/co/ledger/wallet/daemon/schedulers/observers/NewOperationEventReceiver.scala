package co.ledger.wallet.daemon.schedulers.observers

import co.ledger.core._
import co.ledger.wallet.daemon.database.OperationCache
import com.twitter.inject.Logging

import scala.util.{Failure, Success, Try}

class NewOperationEventReceiver(poolName: String, opsCache: OperationCache) extends EventReceiver with Logging {

  override def onEvent(event: Event): Unit =
    if (event.getCode == EventCode.NEW_OPERATION) {
      Try(opsCache.updateOffset(
        poolName,
        event.getPayload.getString(Account.EV_NEW_OP_WALLET_NAME),
        event.getPayload.getInt(Account.EV_NEW_OP_ACCOUNT_INDEX))) match {
        case Success(_) => // Do nothing
        case Failure(e) => error("Failed to update offset with exception", e)
      }
    }

  override def toString: String = s"NewOperationEventReceiver(pool_id: $poolName)"
}
