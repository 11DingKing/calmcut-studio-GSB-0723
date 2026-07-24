package com.calmcut.studio.import

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.domain.model.ImportState
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.event.BatchImported
import com.calmcut.studio.event.EventStore
import com.calmcut.studio.event.ImportChunkTable
import com.calmcut.studio.event.ImportJobTable
import kotlinx.coroutines.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

class StreamingImporter(
    private val eventStore: EventStore,
    private val kafkaTopic: String,
    private val importConfig: AppConfig.ImportConfig
) {
    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    private val activeJobs = mutableMapOf<String, JobControl>()
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    data class JobControl(
        val jobId: String,
        val cancelled: AtomicBoolean = AtomicBoolean(false),
        val completed: CompletableDeferred<ImportState.Status> = CompletableDeferred()
    )

    suspend fun createJob(timelineId: String, totalCount: Long): String {
        val jobId = UUID.randomUUID().toString()
        DatabaseFactory.dbQuery {
            ImportJobTable.insert {
                it[ImportJobTable.jobId] = UUID.fromString(jobId)
                it[ImportJobTable.timelineId] = timelineId
                it[ImportJobTable.totalCount] = totalCount
                it[status] = "pending"
                it[cancelRequested] = false
                it[createdAt] = Instant.now()
                it[updatedAt] = Instant.now()
            }
        }
        activeJobs[jobId] = JobControl(jobId)
        logger.info { "Created import job $jobId for timeline $timelineId, expected $totalCount segments" }
        return jobId
    }

    suspend fun submitChunk(jobId: String, chunkIndex: Int, segments: List<StoryboardSegment>, isLast: Boolean) {
        DatabaseFactory.dbQuery {
            ImportChunkTable.insert {
                it[ImportChunkTable.jobId] = UUID.fromString(jobId)
                it[ImportChunkTable.chunkIndex] = chunkIndex
                it[segmentCount] = segments.size
                it[payload] = json.encodeToString(segments)
                it[status] = "pending"
            }
        }
        if (isLast) {
            startProcessing(jobId)
        }
    }

    suspend fun startProcessing(jobId: String) {
        val control = activeJobs[jobId] ?: return
        val job = scope.launch {
            try {
                updateJobStatus(jobId, "in_progress")
                val timelineId = getTimelineId(jobId)
                var version = eventStore.getLatestVersion(timelineId)
                val chunkSize = importConfig.chunkSize

                while (isActive && !control.cancelled.get()) {
                    val chunks = getNextPendingChunks(jobId, importConfig.maxConcurrentChunks)
                    if (chunks.isEmpty()) break

                    for ((chunkId, chunkIndex, _, payload) in chunks) {
                        if (control.cancelled.get()) {
                            updateJobStatus(jobId, "cancelled")
                            control.completed.complete(ImportState.Status.CANCELLED)
                            return@launch
                        }

                        try {
                            val segments: List<StoryboardSegment> = json.decodeFromString(payload)
                            version++

                            val event = BatchImported(
                                timelineId = timelineId,
                                version = version,
                                jobId = jobId,
                                segments = segments,
                                mode = BatchImported.ImportMode.APPEND
                            )
                            eventStore.append(event, kafkaTopic)

                            markChunkProcessed(chunkId)
                            incrementProcessed(jobId, segments.size)

                            logger.debug { "Job $jobId: processed chunk $chunkIndex with ${segments.size} segments" }
                        } catch (e: Exception) {
                            logger.error(e) { "Job $jobId: failed chunk $chunkIndex" }
                            markChunkFailed(chunkId, e.message ?: "Unknown error")
                            incrementFailed(jobId)
                        }
                    }
                }

                if (control.cancelled.get()) {
                    updateJobStatus(jobId, "cancelled")
                    control.completed.complete(ImportState.Status.CANCELLED)
                } else {
                    updateJobStatus(jobId, "completed")
                    setCompletionTime(jobId)
                    control.completed.complete(ImportState.Status.COMPLETED)
                    logger.info { "Import job $jobId completed successfully" }
                }
            } catch (e: CancellationException) {
                updateJobStatus(jobId, "cancelled")
                control.completed.complete(ImportState.Status.CANCELLED)
                throw e
            } catch (e: Exception) {
                logger.error(e) { "Import job $jobId failed" }
                updateJobFailed(jobId, e.message ?: "Unknown error")
                control.completed.complete(ImportState.Status.FAILED)
            }
        }
        control.completed.invokeOnCompletion { job.cancel() }
    }

    suspend fun cancelJob(jobId: String): Boolean {
        val control = activeJobs[jobId] ?: return false
        control.cancelled.set(true)
        DatabaseFactory.dbQuery {
            ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
                it[cancelRequested] = true
                it[status] = "cancelled"
                it[updatedAt] = Instant.now()
            }
        }
        logger.info { "Cancelled import job $jobId" }
        return true
    }

    suspend fun resumeJob(jobId: String): String? {
        val state = getJobState(jobId) ?: return null
        if (state.status != ImportState.Status.CANCELLED && state.status != ImportState.Status.FAILED) {
            return null
        }

        val control = JobControl(jobId)
        activeJobs[jobId] = control

        DatabaseFactory.dbQuery {
            ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
                it[status] = "in_progress"
                it[cancelRequested] = false
                it[updatedAt] = Instant.now()
            }
        }

        startProcessing(jobId)

        val pendingChunks = getPendingChunkCount(jobId)
        val resumeToken = "job=$jobId&chunks_remaining=$pendingChunks"
        updateResumeToken(jobId, resumeToken)
        logger.info { "Resumed import job $jobId with $pendingChunks pending chunks" }
        return resumeToken
    }

    suspend fun getJobState(jobId: String): ImportState? = DatabaseFactory.dbQuery {
        ImportJobTable.selectAll()
            .where { ImportJobTable.jobId eq UUID.fromString(jobId) }
            .singleOrNull()
            ?.let { row ->
                ImportState(
                    jobId = jobId,
                    timelineId = row[ImportJobTable.timelineId],
                    totalCount = row[ImportJobTable.totalCount],
                    processedCount = row[ImportJobTable.processedCount],
                    failedCount = row[ImportJobTable.failedCount],
                    status = ImportState.Status.valueOf(row[ImportJobTable.status].uppercase()),
                    cancelRequested = row[ImportJobTable.cancelRequested],
                    resumeToken = row[ImportJobTable.resumeToken],
                    errorMessage = row[ImportJobTable.errorMessage]
                )
            }
    }

    private suspend fun getTimelineId(jobId: String): String = DatabaseFactory.dbQuery {
        ImportJobTable.selectAll()
            .where { ImportJobTable.jobId eq UUID.fromString(jobId) }
            .single()[ImportJobTable.timelineId]
    }

    private suspend fun getNextPendingChunks(jobId: String, limit: Int): List<ChunkRow> = DatabaseFactory.dbQuery {
        ImportChunkTable.selectAll()
            .where {
                (ImportChunkTable.jobId eq UUID.fromString(jobId)) and
                    (ImportChunkTable.status eq "pending")
            }
            .orderBy(ImportChunkTable.chunkIndex)
            .limit(limit)
            .map { row ->
                ChunkRow(
                    id = row[ImportChunkTable.id],
                    chunkIndex = row[ImportChunkTable.chunkIndex],
                    segmentCount = row[ImportChunkTable.segmentCount],
                    payload = row[ImportChunkTable.payload]
                )
            }
    }

    private suspend fun getPendingChunkCount(jobId: String): Int = DatabaseFactory.dbQuery {
        ImportChunkTable.selectAll()
            .where {
                (ImportChunkTable.jobId eq UUID.fromString(jobId)) and
                    (ImportChunkTable.status eq "pending")
            }
            .count().toInt()
    }

    private data class ChunkRow(val id: Long, val chunkIndex: Int, val segmentCount: Int, val payload: String)

    private suspend fun markChunkProcessed(id: Long) = DatabaseFactory.dbQuery {
        ImportChunkTable.update({ ImportChunkTable.id eq id }) {
            it[status] = "completed"
            it[processedAt] = Instant.now()
        }
    }

    private suspend fun markChunkFailed(id: Long, error: String) = DatabaseFactory.dbQuery {
        ImportChunkTable.update({ ImportChunkTable.id eq id }) {
            it[status] = "failed"
            it[errorMessage] = error
            it[processedAt] = Instant.now()
        }
    }

    private suspend fun updateJobStatus(jobId: String, status: String) = DatabaseFactory.dbQuery {
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[ImportJobTable.status] = status
            it[updatedAt] = Instant.now()
        }
    }

    private suspend fun updateJobFailed(jobId: String, error: String) = DatabaseFactory.dbQuery {
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[status] = "failed"
            it[errorMessage] = error
            it[updatedAt] = Instant.now()
            it[completedAt] = Instant.now()
        }
    }

    private suspend fun incrementProcessed(jobId: String, count: Int) = DatabaseFactory.dbQuery {
        val current = ImportJobTable.selectAll()
            .where { ImportJobTable.jobId eq UUID.fromString(jobId) }
            .single()[ImportJobTable.processedCount]
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[processedCount] = current + count
            it[updatedAt] = Instant.now()
        }
    }

    private suspend fun incrementFailed(jobId: String) = DatabaseFactory.dbQuery {
        val current = ImportJobTable.selectAll()
            .where { ImportJobTable.jobId eq UUID.fromString(jobId) }
            .single()[ImportJobTable.failedCount]
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[failedCount] = current + 1
            it[updatedAt] = Instant.now()
        }
    }

    private suspend fun setCompletionTime(jobId: String) = DatabaseFactory.dbQuery {
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[completedAt] = Instant.now()
            it[updatedAt] = Instant.now()
        }
    }

    private suspend fun updateResumeToken(jobId: String, token: String) = DatabaseFactory.dbQuery {
        ImportJobTable.update({ ImportJobTable.jobId eq UUID.fromString(jobId) }) {
            it[resumeToken] = token
        }
    }

    fun shutdown() {
        scope.cancel()
    }
}
