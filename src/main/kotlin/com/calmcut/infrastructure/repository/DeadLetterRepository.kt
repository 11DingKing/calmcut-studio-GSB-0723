package com.calmcut.infrastructure.repository

import com.calmcut.infrastructure.db.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import java.util.UUID

data class DeadLetterRecord(
    val id: Long,
    val originalTopic: String,
    val originalPartition: Int,
    val originalOffset: Long,
    val eventType: String,
    val storyboardId: String?,
    val payload: String,
    val errorMessage: String,
    val errorStackTrace: String?,
    val retryCount: Int,
    val maxRetries: Int,
    val nextRetryAt: Long?,
    val resolved: Boolean
)

class DeadLetterRepository {

    suspend fun save(
        originalTopic: String,
        originalPartition: Int,
        originalOffset: Long,
        eventType: String,
        storyboardId: String?,
        payload: String,
        errorMessage: String,
        errorStackTrace: String?,
        maxRetries: Int = 3
    ): Long = newSuspendedTransaction {
        DeadLetters.insertAndGetId {
            it[DeadLetters.originalTopic] = originalTopic
            it[DeadLetters.originalPartition] = originalPartition
            it[DeadLetters.originalOffset] = originalOffset
            it[DeadLetters.eventType] = eventType
            it[DeadLetters.storyboardId] = storyboardId?.let { UUID.fromString(it) }
            it[DeadLetters.payload] = payload
            it[DeadLetters.errorMessage] = errorMessage
            it[DeadLetters.errorStackTrace] = errorStackTrace
            it[retryCount] = 0
            it[DeadLetters.maxRetries] = maxRetries
            it[nextRetryAt] = Instant.now().plusSeconds(30)
            it[resolved] = false
            it[createdAt] = Instant.now()
            it[updatedAt] = Instant.now()
        }.value
    }

    suspend fun getRetryableCandidates(limit: Int = 50): List<DeadLetterRecord> = newSuspendedTransaction {
        DeadLetters.selectAll()
            .where {
                (DeadLetters.resolved eq false) and
                (DeadLetters.retryCount less DeadLetters.maxRetries) and
                (DeadLetters.nextRetryAt lessEq Instant.now())
            }
            .orderBy(DeadLetters.nextRetryAt, SortOrder.ASC)
            .limit(limit)
            .map { it.toDeadLetterRecord() }
    }

    suspend fun markRetried(id: Long, success: Boolean, error: String? = null): Unit = newSuspendedTransaction {
        val record = DeadLetters.selectAll().where { DeadLetters.id eq id }.firstOrNull() ?: return@newSuspendedTransaction
        val newRetryCount = record[DeadLetters.retryCount] + 1
        val maxRetries = record[DeadLetters.maxRetries]

        DeadLetters.update({ DeadLetters.id eq id }) {
            if (success) {
                it[resolved] = true
            } else {
                it[retryCount] = newRetryCount
                it[errorMessage] = error ?: record[DeadLetters.errorMessage]
                if (newRetryCount < maxRetries) {
                    val backoffSeconds = 30L * (1L shl newRetryCount.coerceAtMost(5))
                    it[nextRetryAt] = Instant.now().plusSeconds(backoffSeconds)
                } else {
                    it[nextRetryAt] = null
                }
            }
            it[updatedAt] = Instant.now()
        }
    }

    suspend fun getUnresolvedCount(): Long = newSuspendedTransaction {
        DeadLetters.selectAll()
            .where { DeadLetters.resolved eq false }
            .count()
    }

    suspend fun resolve(id: Long): Unit = newSuspendedTransaction {
        DeadLetters.update({ DeadLetters.id eq id }) {
            it[resolved] = true
            it[updatedAt] = Instant.now()
        }
    }

    private fun ResultRow.toDeadLetterRecord(): DeadLetterRecord = DeadLetterRecord(
        id = this[DeadLetters.id].value,
        originalTopic = this[DeadLetters.originalTopic],
        originalPartition = this[DeadLetters.originalPartition],
        originalOffset = this[DeadLetters.originalOffset],
        eventType = this[DeadLetters.eventType],
        storyboardId = this[DeadLetters.storyboardId]?.toString(),
        payload = this[DeadLetters.payload],
        errorMessage = this[DeadLetters.errorMessage],
        errorStackTrace = this[DeadLetters.errorStackTrace],
        retryCount = this[DeadLetters.retryCount],
        maxRetries = this[DeadLetters.maxRetries],
        nextRetryAt = this[DeadLetters.nextRetryAt]?.toEpochMilli(),
        resolved = this[DeadLetters.resolved]
    )
}

class ConsumerOffsetRepository {

    suspend fun getOffset(consumerGroup: String, topicPartition: String): Long = newSuspendedTransaction {
        ConsumerOffsets.select(ConsumerOffsets.offsetVal)
            .where {
                (ConsumerOffsets.consumerGroup eq consumerGroup) and
                (ConsumerOffsets.topicPartition eq topicPartition)
            }
            .firstOrNull()
            ?.get(ConsumerOffsets.offsetVal) ?: 0L
    }

    suspend fun saveOffset(consumerGroup: String, topicPartition: String, offset: Long, eventId: Long?): Unit =
        newSuspendedTransaction {
            val existing = ConsumerOffsets.selectAll()
                .where {
                    (ConsumerOffsets.consumerGroup eq consumerGroup) and
                    (ConsumerOffsets.topicPartition eq topicPartition)
                }
                .firstOrNull()

            if (existing != null) {
                ConsumerOffsets.update({
                    (ConsumerOffsets.consumerGroup eq consumerGroup) and
                    (ConsumerOffsets.topicPartition eq topicPartition)
                }) {
                    it[offsetVal] = offset
                    it[lastEventId] = eventId
                    it[updatedAt] = Instant.now()
                }
            } else {
                ConsumerOffsets.insert {
                    it[ConsumerOffsets.consumerGroup] = consumerGroup
                    it[ConsumerOffsets.topicPartition] = topicPartition
                    it[offsetVal] = offset
                    it[lastEventId] = eventId
                    it[updatedAt] = Instant.now()
                }
            }
        }
}
