package com.calmcut.infrastructure.repository

import com.calmcut.domain.events.SegmentData
import com.calmcut.infrastructure.db.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import java.util.UUID

private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

enum class ImportJobStatus {
    PENDING, RUNNING, PAUSED, COMPLETED, CANCELLED, FAILED
}

data class ImportJobRecord(
    val id: String,
    val storyboardId: String,
    val status: ImportJobStatus,
    val totalSegments: Int,
    val processedSegments: Int,
    val failedSegments: Int,
    val checkpoint: Int,
    val startedAt: Long?,
    val completedAt: Long?,
    val cancelledAt: Long?,
    val errorMessage: String?,
    val metadata: Map<String, String>
)

class ImportRepository {

    suspend fun createJob(
        storyboardId: String,
        totalSegments: Int,
        metadata: Map<String, String> = emptyMap()
    ): String = newSuspendedTransaction {
        val id = UUID.randomUUID()
        val now = Instant.now()
        ImportJobs.insert {
            it[ImportJobs.id] = id
            it[ImportJobs.storyboardId] = UUID.fromString(storyboardId)
            it[status] = ImportJobStatus.PENDING.name
            it[ImportJobs.totalSegments] = totalSegments
            it[processedSegments] = 0
            it[failedSegments] = 0
            it[checkpoint] = 0
            it[jobMetadata] = json.encodeToString(metadata)
            it[createdAt] = now
            it[updatedAt] = now
        }
        id.toString()
    }

    suspend fun insertBatches(jobId: String, batches: List<List<SegmentData>>): Unit = newSuspendedTransaction {
        val jobUUID = UUID.fromString(jobId)
        val now = Instant.now()
        batches.forEachIndexed { index, batch ->
            ImportBatches.insert {
                it[importJobId] = jobUUID
                it[batchNumber] = index
                it[segmentData] = json.encodeToString(batch)
                it[processed] = false
                it[createdAt] = now
            }
        }
        ImportJobs.update({ ImportJobs.id eq jobUUID }) {
            it[status] = ImportJobStatus.RUNNING.name
            it[startedAt] = now
            it[updatedAt] = now
        }
    }

    suspend fun getNextUnprocessedBatch(jobId: String): Pair<Int, List<SegmentData>>? = newSuspendedTransaction {
        ImportBatches.selectAll()
            .where {
                (ImportBatches.importJobId eq UUID.fromString(jobId)) and
                (ImportBatches.processed eq false)
            }
            .orderBy(ImportBatches.batchNumber, SortOrder.ASC)
            .limit(1)
            .firstOrNull()
            ?.let { row ->
                val batchNum = row[ImportBatches.batchNumber]
                val data = json.decodeFromString<List<SegmentData>>(row[ImportBatches.segmentData])
                batchNum to data
            }
    }

    suspend fun markBatchProcessed(jobId: String, batchNumber: Int, success: Boolean, error: String? = null): Unit =
        newSuspendedTransaction {
            val jobUUID = UUID.fromString(jobId)
            ImportBatches.update({
                (ImportBatches.importJobId eq jobUUID) and (ImportBatches.batchNumber eq batchNumber)
            }) {
                it[processed] = true
                it[processedAt] = Instant.now()
                it[errorMessage] = error
            }
            val currentJob = ImportJobs.selectAll()
                .where { ImportJobs.id eq jobUUID }
                .firstOrNull() ?: return@newSuspendedTransaction
            val currentProcessed = currentJob[ImportJobs.processedSegments]
            val currentFailed = currentJob[ImportJobs.failedSegments]
            ImportJobs.update({ ImportJobs.id eq jobUUID }) {
                it[processedSegments] = if (success) currentProcessed + 1 else currentProcessed
                it[failedSegments] = if (!success) currentFailed + 1 else currentFailed
                it[checkpoint] = batchNumber + 1
                it[updatedAt] = Instant.now()
            }
        }

    suspend fun updateJobStatus(jobId: String, status: ImportJobStatus, error: String? = null): Unit =
        newSuspendedTransaction {
            val now = Instant.now()
            ImportJobs.update({ ImportJobs.id eq UUID.fromString(jobId) }) {
                it[ImportJobs.status] = status.name
                when (status) {
                    ImportJobStatus.COMPLETED -> it[completedAt] = now
                    ImportJobStatus.CANCELLED -> it[cancelledAt] = now
                    ImportJobStatus.FAILED -> it[errorMessage] = error
                    else -> {}
                }
                it[updatedAt] = now
            }
        }

    suspend fun getJob(jobId: String): ImportJobRecord? = newSuspendedTransaction {
        ImportJobs.selectAll()
            .where { ImportJobs.id eq UUID.fromString(jobId) }
            .firstOrNull()
            ?.toImportJobRecord()
    }

    suspend fun getActiveJobs(): List<ImportJobRecord> = newSuspendedTransaction {
        ImportJobs.selectAll()
            .where { ImportJobs.status inList listOf(ImportJobStatus.PENDING.name, ImportJobStatus.RUNNING.name) }
            .map { it.toImportJobRecord() }
    }

    suspend fun isCancelled(jobId: String): Boolean = newSuspendedTransaction {
        ImportJobs.select(ImportJobs.status)
            .where { ImportJobs.id eq UUID.fromString(jobId) }
            .firstOrNull()
            ?.get(ImportJobs.status) == ImportJobStatus.CANCELLED.name
    }

    suspend fun getCheckpoint(jobId: String): Int = newSuspendedTransaction {
        ImportJobs.select(ImportJobs.checkpoint)
            .where { ImportJobs.id eq UUID.fromString(jobId) }
            .firstOrNull()
            ?.get(ImportJobs.checkpoint) ?: 0
    }

    suspend fun resumeFromCheckpoint(jobId: String): Int = newSuspendedTransaction {
        val job = ImportJobs.selectAll()
            .where { ImportJobs.id eq UUID.fromString(jobId) }
            .firstOrNull() ?: return@newSuspendedTransaction 0

        ImportJobs.update({ ImportJobs.id eq UUID.fromString(jobId) }) {
            it[status] = ImportJobStatus.RUNNING.name
            it[updatedAt] = Instant.now()
        }
        job[ImportJobs.checkpoint]
    }

    private fun ResultRow.toImportJobRecord(): ImportJobRecord = ImportJobRecord(
        id = this[ImportJobs.id].toString(),
        storyboardId = this[ImportJobs.storyboardId].toString(),
        status = ImportJobStatus.valueOf(this[ImportJobs.status]),
        totalSegments = this[ImportJobs.totalSegments],
        processedSegments = this[ImportJobs.processedSegments],
        failedSegments = this[ImportJobs.failedSegments],
        checkpoint = this[ImportJobs.checkpoint],
        startedAt = this[ImportJobs.startedAt]?.toEpochMilli(),
        completedAt = this[ImportJobs.completedAt]?.toEpochMilli(),
        cancelledAt = this[ImportJobs.cancelledAt]?.toEpochMilli(),
        errorMessage = this[ImportJobs.errorMessage],
        metadata = json.decodeFromString<Map<String, String>>(this[ImportJobs.jobMetadata])
    )
}
