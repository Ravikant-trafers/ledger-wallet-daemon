package co.ledger.wallet.daemon.controllers.requests

import co.ledger.wallet.daemon.models.{AccountInfo, PoolInfo, TokenAccountInfo, WalletInfo}

trait WithTokenAccountInfo extends WithAccountInfo {
  def token_address: String

  def tokenAccountInfo: TokenAccountInfo = TokenAccountInfo(token_address, accountInfo)
}

trait WithWalletInfo extends WithPoolInfo {
  def wallet_name: String

  def walletInfo: WalletInfo = WalletInfo(wallet_name, poolInfo)
}

trait WithAccountInfo extends WithWalletInfo {
  def account_index: Int

  def accountInfo: AccountInfo = AccountInfo(account_index, walletInfo)
}

trait WithPoolInfo {
  def pool_name: String

  def poolInfo: PoolInfo = PoolInfo(pool_name)
}
