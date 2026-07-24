package com.calmcut.infrastructure.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.server.config.*
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.jetbrains.exposed.sql.transactions.transaction
import javax.sql.DataSource

class DatabaseFactory(private val config: ApplicationConfig) {

    private lateinit var dataSource: HikariDataSource

    fun connect(): Database {
        val dbConfig = config.config("database")
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = dbConfig.property("jdbcUrl").getString()
            driverClassName = dbConfig.property("driverClassName").getString()
            username = dbConfig.property("username").getString()
            password = dbConfig.property("password").getString()
            maximumPoolSize = dbConfig.property("maximumPoolSize").getString().toInt()
            isAutoCommit = false
            transactionIsolation = "TRANSACTION_REPEATABLE_READ"
            validate()
        }
        dataSource = HikariDataSource(hikariConfig)

        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .baselineOnMigrate(true)
            .load()
            .migrate()

        return Database.connect(dataSource)
    }

    fun close() {
        if (::dataSource.isInitialized) {
            dataSource.close()
        }
    }

    companion object {
        suspend fun <T> dbQuery(block: suspend () -> T): T =
            newSuspendedTransaction { block() }
    }
}
