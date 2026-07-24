package com.calmcut.studio.db

import com.calmcut.studio.config.AppConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.jetbrains.exposed.sql.transactions.transaction

/** Owns the Hikari pool + Exposed Database and exposes a coroutine-friendly tx helper. */
class DatabaseFactory(private val config: AppConfig.DbConfig) {

    lateinit var database: Database
        private set

    private lateinit var dataSource: HikariDataSource

    fun connect() {
        val hikari = HikariConfig().apply {
            jdbcUrl = config.jdbcUrl
            username = config.username
            password = config.password
            maximumPoolSize = config.maxPoolSize
            driverClassName = "org.postgresql.Driver"
            isAutoCommit = false
            transactionIsolation = "TRANSACTION_READ_COMMITTED"
        }
        dataSource = HikariDataSource(hikari)
        database = Database.connect(dataSource)
        transaction(database) {
            SchemaUtils.createMissingTablesAndColumns(
                StoryboardEvents, Outbox, StoryboardHead, SegmentProjection,
                AnalysisProjection, ProcessedVersion, ProcessedEvent, DeadLetter, ImportJobs,
                PendingEvents, ImportSegments, ReplayAudit,
            )
        }
    }

    suspend fun <T> dbQuery(block: suspend () -> T): T =
        newSuspendedTransaction(Dispatchers.IO, database) { block() }

    fun close() {
        if (::dataSource.isInitialized) dataSource.close()
    }
}
