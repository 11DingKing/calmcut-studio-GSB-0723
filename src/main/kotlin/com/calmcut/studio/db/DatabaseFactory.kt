package com.calmcut.studio.db

import com.calmcut.studio.config.AppConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.jetbrains.exposed.sql.transactions.transaction
import javax.sql.DataSource

object DatabaseFactory {
    private lateinit var dataSource: HikariDataSource

    fun init(config: AppConfig.DbConfig): DataSource {
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = config.jdbcUrl
            username = config.username
            password = config.password
            maximumPoolSize = config.maximumPoolSize
            isAutoCommit = false
            transactionIsolation = "TRANSACTION_READ_COMMITTED"
            driverClassName = "org.postgresql.Driver"
            validate()
        }
        dataSource = HikariDataSource(hikariConfig)
        Database.connect(dataSource)

        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .baselineOnMigrate(true)
            .load()
            .migrate()

        return dataSource
    }

    fun getDataSource(): DataSource = dataSource

    suspend fun <T> dbQuery(block: suspend () -> T): T =
        withContext(Dispatchers.IO) {
            newSuspendedTransaction { block() }
        }

    fun <T> dbQueryBlocking(block: () -> T): T =
        transaction { block() }

    fun close() {
        if (::dataSource.isInitialized) dataSource.close()
    }
}
