package com.calmcut.studio.worker

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.event.DeadLetterTable
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import java.time.Instant
import java.util.UUID

class DeadLetterQueue(private val consumerGroup: String) : DeadLetterSink {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    override suspend fun enqueue(
        eventId: String,
        topic: String,
        partition: Int,
        offset: Long,
        payload: String,
        error: Throwable
    ): Long = DatabaseFactory.dbQuery {
        val id = DeadLetterTable.insert {
            it[DeadLetterTable.eventId] = UUID.fromString(eventId)
            it[DeadLetterTable.consumerGroup] = this@DeadLetterQueue.consumerGroup
            it[DeadLetterTable.topic] = topic
            it[DeadLetterTable.partition] = partition
            it[DeadLetterTable.offset] = offset
            it[DeadLetterTable.payload] = payload
            it[errorMessage] = error.message ?: error.toString()
            it[errorClass] = error::class.qualifiedName
            it[attemptCount] = 1
            it[replayed] = false
            it[createdAt] = Instant.now()
        } get DeadLetterTable.id
        id
    }

    override suspend fun getUnreplayed(limit: Int): List<DeadLetterSink.DlqEntry> = DatabaseFactory.dbQuery {
        DeadLetterTable.selectAll()
            .where {
                (DeadLetterTable.consumerGroup eq consumerGroup) and
                    (DeadLetterTable.replayed eq false)
            }
            .orderBy(DeadLetterTable.createdAt)
            .limit(limit)
            .map { row ->
                DeadLetterSink.DlqEntry(
                    id = row[DeadLetterTable.id],
                    eventId = row[DeadLetterTable.eventId].toString(),
                    topic = row[DeadLetterTable.topic],
                    partition = row[DeadLetterTable.partition],
                    offset = row[DeadLetterTable.offset],
                    payload = row[DeadLetterTable.payload],
                    errorMessage = row[DeadLetterTable.errorMessage],
                    errorClass = row[DeadLetterTable.errorClass],
                    attemptCount = row[DeadLetterTable.attemptCount]
                )
            }
    }

    override suspend fun markReplayed(id: Long) {
        DatabaseFactory.dbQuery {
            DeadLetterTable.update({ DeadLetterTable.id eq id }) {
                it[replayed] = true
                it[replayedAt] = Instant.now()
            }
        }
    }

    override suspend fun incrementAttempt(id: Long) {
        DatabaseFactory.dbQuery {
            val current = DeadLetterTable.selectAll()
                .where { DeadLetterTable.id eq id }
                .single()[DeadLetterTable.attemptCount]
            DeadLetterTable.update({ DeadLetterTable.id eq id }) {
                it[attemptCount] = current + 1
            }
        }
    }

    suspend fun clearReplayed() = DatabaseFactory.dbQuery {
        DeadLetterTable.deleteWhere {
            (DeadLetterTable.consumerGroup eq consumerGroup) and
                (DeadLetterTable.replayed eq true)
        }
    }
}
