package co.ledger.wallet.daemon.api

import co.ledger.wallet.daemon.controllers.TransactionsController.CreateXTZTransactionRequest
import co.ledger.core.TezosOperationTag
import co.ledger.wallet.daemon.models.FreshAddressView
import co.ledger.wallet.daemon.models.coins.UnsignedTezosTransactionView
import co.ledger.wallet.daemon.services.OperationQueryParams
import co.ledger.wallet.daemon.utils.APIFeatureTest
import com.fasterxml.jackson.databind.JsonNode
import com.twitter.finagle.http.Status

class XTZAccountsApiTest extends APIFeatureTest {

  val poolName = "tez_test_pool"

  override def beforeEach(): Unit = {
    createPool(poolName)
  }

  override def afterEach(): Unit = {
    deletePool(poolName)
  }

  private val CORRECT_BODY_XTZ =
    """{
      "account_index": 0,
      "derivations": [
        {
          "owner": "main",
          "path": "44'/1729'/0'",
          "pub_key": "04AF5696511E23B9E3DC5A527ABC6929FAE708DEFB5299F96CFA7DD9F936FE747DEA6650574572F94869DBE7DF30A5CBD77C5579E5D1BA2867EF8032EE87259B40",
          "chain_code": ""
        }
      ]
    }"""

  test("Create XTZ account") {
    val walletName = "xtzWalletAccountCreation"
    assertWalletCreation(poolName, walletName, "tezos", Status.Ok)
    assertCreateAccount(CORRECT_BODY_XTZ, poolName, walletName, Status.Ok)
    val addresses = parse[Seq[FreshAddressView]](assertGetFreshAddresses(poolName, walletName, index = 0, Status.Ok))
    assert(addresses.nonEmpty)
    info(s"Here are addresses : $addresses")
    assertSyncAccount(poolName, walletName, 0)
    val operations = parse[Map[String, JsonNode]](assertGetAccountOps(poolName, walletName, 0, OperationQueryParams(None, None, 1000, 0), Status.Ok))
    assert(operations.nonEmpty)
  }

  test("Create XTZ Transaction") {
    val walletName = "xtzWalletForCreateTX"
    assertWalletCreation(poolName, walletName, "tezos", Status.Ok)
    assertCreateAccount(CORRECT_BODY_XTZ, poolName, walletName, Status.Ok)
    val addresses = parse[Seq[FreshAddressView]](assertGetFreshAddresses(poolName, walletName, index = 0, Status.Ok))
    assert(addresses.nonEmpty)
    info(s"Here are addresses : $addresses")
    assertSyncAccount(poolName, walletName, 0)
    val operations = parse[Map[String, JsonNode]](assertGetAccountOps(poolName, walletName, 0, OperationQueryParams(None, None, 1000, 0), Status.Ok))
    assert(operations.nonEmpty)

    // No fees, no feesLevel provided
    val add = addresses.head.address
    val receiverAddress = "tz2LLBZYevBRjNBvzJ24GbAkJ5bNFDQi3KQv"

    val txBadRequest = CreateXTZTransactionRequest(TezosOperationTag.OPERATION_TAG_TRANSACTION, receiverAddress,
                                                   "1000", false, Some("800"), Some("800"), None, None)
    val txBadRequestJson = server.mapper.objectMapper.writeValueAsString(txBadRequest)
    // Neither fees nor fees_level has been provided, expect failure due to MethodValidation check
    assertCreateTransaction(txBadRequestJson, poolName, walletName, 0, Status.BadRequest)

    // Not enough funds error
    val txTooPoorRequest = CreateXTZTransactionRequest(TezosOperationTag.OPERATION_TAG_TRANSACTION, receiverAddress,
                                                       "80000000", false, Some("800"), Some("800"), None, Some("FAST"))
    val txTooPoorRequestJson = server.mapper.objectMapper.writeValueAsString(txTooPoorRequest)
    assertCreateTransaction(txTooPoorRequestJson, poolName, walletName, 0, Status.BadRequest)

    // Check No fees amount provided with fee_level provided
    val txRequest = CreateXTZTransactionRequest(TezosOperationTag.OPERATION_TAG_TRANSACTION, receiverAddress,
                                                "1", false, Some("8"), Some("8"), None, Some("SLOW"))
    val txRequestJson = server.mapper.objectMapper.writeValueAsString(txRequest)
    val transactionView = parse[UnsignedTezosTransactionView](assertCreateTransaction(txRequestJson, poolName, walletName, 0, Status.Ok))
    info(s"Here is transaction view : $transactionView")
    assert(transactionView.operationType == TezosOperationTag.OPERATION_TAG_TRANSACTION)
    assert(transactionView.value == "1")
    assert(transactionView.signing_pubkey == "04AF5696511E23B9E3DC5A527ABC6929FAE708DEFB5299F96CFA7DD9F936FE747DEA6650574572F94869DBE7DF30A5CBD77C5579E5D1BA2867EF8032EE87259B40")
    assert(transactionView.receiver == Some(receiverAddress))
    assert(transactionView.sender == add)

    // Check normal speed fees multiplier
    val txNormalSpeedRequest = CreateXTZTransactionRequest(TezosOperationTag.OPERATION_TAG_TRANSACTION, receiverAddress,
                                                           "1", false, Some("8"), Some("8"), Some("322"), Some("NORMAL"))
    val txNormalSpeedRequestJson = server.mapper.objectMapper.writeValueAsString(txNormalSpeedRequest)
    val normalSpeedTransactionView = parse[UnsignedTezosTransactionView](assertCreateTransaction(txNormalSpeedRequestJson, poolName, walletName, 0, Status.Ok))
    info(s"Here is transaction view : $normalSpeedTransactionView")
    assert(normalSpeedTransactionView.fees == Some("644"))

    // Check fast speed multiplier
    val txFastSpeedRequest = CreateXTZTransactionRequest(TezosOperationTag.OPERATION_TAG_TRANSACTION, receiverAddress,
                                                         "1", false, Some("8"), Some("8"), Some("923"), Some("NORMAL"))
    val txFastSpeedRequestJson = server.mapper.objectMapper.writeValueAsString(txFastSpeedRequest)
    val fastSpeedTransactionView = parse[UnsignedTezosTransactionView](assertCreateTransaction(txFastSpeedRequestJson, poolName, walletName, 0, Status.Ok))
    info(s"Here is transaction view : $fastSpeedTransactionView")
    assert(fastSpeedTransactionView.fees == Some("1846"))
  }
}