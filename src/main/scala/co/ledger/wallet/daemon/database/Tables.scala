package co.ledger.wallet.daemon.database

import java.sql.Timestamp

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import slick.lifted.ProvenShape
import slick.sql.SqlProfile.ColumnOption.SqlType

object Tables extends Tables {
  override val profile = DaemonConfiguration.dbProfile
}

trait Tables {
  val profile: slick.jdbc.JdbcProfile
  import profile.api._

  class DatabaseVersion(tag: Tag) extends Table[(Int, Timestamp)](tag, "__database__") {

    def version: Rep[Int] = column[Int]("version", O.PrimaryKey)

    def createdAt: Rep[Timestamp] = column[Timestamp]("created_at", SqlType("timestamp not null default CURRENT_TIMESTAMP"))

    override def * : ProvenShape[(Int, Timestamp)] = (version, createdAt)
  }

  val databaseVersions = TableQuery[DatabaseVersion]

  case class PoolRow(name: String)

  class Pools(tag: Tag) extends Table[PoolRow](tag, "pools") {
    def name: Rep[String] = column[String]("name", O.PrimaryKey)

    def * : ProvenShape[PoolRow] = (name) <> (PoolRow.apply, PoolRow.unapply)
  }

  val pools = TableQuery[Pools]
}
