package com.calmcut.studio.import

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.db.ImportJobs
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory

enum class ImportStatus { RUNNING, COMPLETED, CANCELLED, FAILED }

data class ImportProgress(
    val jobId: String,
    val storyboardId: String,
    val status: ImportStatus,
    val total: Long,
    val processed: Long,
)

/**
 * Streams a very large segment source (up to millions of "分镜") into a storyboard
 * in bounded-memory chunks ("百万分镜流式导入"). Each chunk is appended as one
 * SEGMENT_CREATED event within its own transaction, and a checkpoint (last
 * offset + processed count) is persisted after every chunk so an interrupted or
 * cancelled job can resume from where it stopped ("取消恢复").
 *
 * A cooperative cancel flag is checked between chunks; when set, the job stops
 * cleanly and can later be resumed by re-invoking with the same job id.
 */
class StreamingImportService(
    private val db: DatabaseFactory,
    private val eventStore: EventStore,
    private val chunkSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Creates a new import job row and returns its id. */
    suspend fun createJob(storyboardId: String, total: Long): String = db.dbQuery {
        val id = UUID.randomUUID().toString()
        ImportJobs.insert {
            it[jobId] = id
            it[this.storyboardId] = storyboardId
            it[status] = ImportStatus.RUNNING.name
            it[totalSegments] = total
            it[processedSegments] = 0
            it[lastOffset] = 0
            it[cancelRequested] = false
            it[createdAt] = Instant.now()
            it[updatedAt] = Instant.now()
        }
        id
    }

    suspend fun requestCancel(jobId: String): Boolean = db.dbQuery {
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[cancelRequested] = true
            it[updatedAt] = Instant.now()
        } > 0
    }

    suspend fun progress(jobId: String): ImportProgress? = db.dbQuery {
        ImportJobs.selectAll().where { ImportJobs.jobId eq jobId }.firstOrNull()?.let {
            ImportProgress(
                jobId = it[ImportJobs.jobId],
                storyboardId = it[ImportJobs.storyboardId],
                status = ImportStatus.valueOf(it[ImportJobs.status]),
                total = it[ImportJobs.totalSegments],
                processed = it[ImportJobs.processedSegments],
            )
        }
    }

    /**
     * Runs (or resumes) an import. Segments already applied (index < lastOffset
     * checkpoint) are skipped, making the operation restartable. The [segments]
     * flow is consumed lazily so memory stays bounded regardless of total size.
     */
    suspend fun run(jobId: String, segments: Flow<Segment>): ImportProgress {
        val start = progress(jobId) ?: error("import job $jobId not found")
        if (start.status == ImportStatus.COMPLETED) return start

        val storyboardId = start.storyboardId
        val resumeFrom = start.processed

        segments.collectChunked(chunkSize, skip = resumeFrom) { batch, consumed ->
            if (isCancelled(jobId)) {
                markStatus(jobId, ImportStatus.CANCELLED)
                return@collectChunked false // stop collecting
            }
            appendChunk(storyboardId, batch)
            checkpoint(jobId, consumed)
            true
        }

        val finalState = progress(jobId)!!
        return if (finalState.status == ImportStatus.CANCELLED) {
            finalState
        } else {
            markStatus(jobId, ImportStatus.COMPLETED)
            progress(jobId)!!
        }
    }

    private suspend fun appendChunk(storyboardId: String, batch: List<Segment>) {
        if (batch.isEmpty()) return
        db.dbQuery {
            val expected = eventStore.currentHeadVersion(storyboardId)
            eventStore.append(
                StoryboardEvent(
                    eventId = UUID.randomUUID().toString(),
                    storyboardId = storyboardId,
                    version = 0, // assigned by the store
                    type = EventType.SEGMENT_CREATED,
                    segments = batch,
                ),
                expectedVersion = expected,
            )
        }
    }

    private suspend fun isCancelled(jobId: String): Boolean = db.dbQuery {
        ImportJobs.selectAll().where { ImportJobs.jobId eq jobId }
            .firstOrNull()?.get(ImportJobs.cancelRequested) ?: false
    }

    private suspend fun checkpoint(jobId: String, processed: Long) = db.dbQuery {
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[processedSegments] = processed
            it[lastOffset] = processed
            it[updatedAt] = Instant.now()
        }
    }

    private suspend fun markStatus(jobId: String, status: ImportStatus) = db.dbQuery {
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[this.status] = status.name
            it[updatedAt] = Instant.now()
        }
    }
}
