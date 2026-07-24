package com.calmcut.studio.worker

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.event.ProcessedEventsTable
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import java.time.Instant
import java.util.UUID

class IdempotentProcessor(private val consumerGroup: String) : IdempotencyChecker {

    override suspend fun isProcessed(eventId: String): Boolean = DatabaseFactory.dbQuery {
        ProcessedEventsTable.selectAll()
            .where {
                (ProcessedEventsTable.consumerGroup eq consumerGroup) and
                    (ProcessedEventsTable.eventId eq UUID.fromString(eventId))
            }
            .count() > 0
    }

    override suspend fun markProcessed(eventId: String, result: String) = DatabaseFactory.dbQuery {
        val exists = ProcessedEventsTable.selectAll()
            .where {
                (ProcessedEventsTable.consumerGroup eq consumerGroup) and
                    (ProcessedEventsTable.eventId eq UUID.fromString(eventId))
            }
            .count() > 0

        if (!exists) {
            ProcessedEventsTable.insert {
                it[ProcessedEventsTable.consumerGroup] = this@IdempotentProcessor.consumerGroup
                it[ProcessedEventsTable.eventId] = UUID.fromString(eventId)
                it[processedAt] = Instant.now()
                it[ProcessedEventsTable.result] = result
            }
        }
    }

    override suspend fun markFailed(eventId: String) {
        DatabaseFactory.dbQuery {
            ProcessedEventsTable.deleteWhere {
                (ProcessedEventsTable.consumerGroup eq consumerGroup) and
                    (ProcessedEventsTable.eventId eq UUID.fromString(eventId))
            }
        }
    }

    override suspend fun resetAll() {
        DatabaseFactory.dbQuery {
            ProcessedEventsTable.deleteWhere { ProcessedEventsTable.consumerGroup eq consumerGroup }
        }
    }
}
