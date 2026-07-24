package com.calmcut.studio.import

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.db.ImportJobs
import com.calmcut.studio.db.ImportSegments
import com.calmcut.studio.db.storyboardJson
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory

enum class ImportStatus { INGESTING, RUNNING, COMPLETED, CANCELLED, FAILED }

data class ImportProgress(
    val jobId: String,
    val storyboardId: String,
    val status: ImportStatus,
    val total: Long,
    val processed: Long,
)

/**
 * Streams up to millions of "分镜" into a storyboard without ever holding the
 * whole set in memory. There are two phases, both bounded-memory:
 *
 *  1. **Ingest** — the incoming [Flow] of segments (in production, parsed lazily
 *     from the streamed NDJSON request body, giving real backpressure) is written
 *     straight into the durable `import_segment` staging table in bounded batches.
 *     Nothing is materialised into a big in-memory list.
 *  2. **Apply** — the importer reads staged rows back from the table by offset,
 *     appends each chunk as one event, and checkpoints `processed`/`last_offset`
 *     after every chunk.
 *
 * Cancel sets a durable flag checked between chunks. Resume re-launches the apply
 * phase from the persisted checkpoint against the persistent staging source, so
 * it genuinely continues the job rather than returning a placeholder.
 */
class StreamingImportService(
    private val db: DatabaseFactory,
    private val eventStore: EventStore,
    private val chunkSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun createJob(storyboardId: String): String = db.dbQuery {
        val id = UUID.randomUUID().toString()
        ImportJobs.insert {
            it[jobId] = id
            it[this.storyboardId] = storyboardId
            it[status] = ImportStatus.INGESTING.name
            it[totalSegments] = 0
            it[processedSegments] = 0
            it[lastOffset] = 0
            it[cancelRequested] = false
            it[createdAt] = Instant.now()
            it[updatedAt] = Instant.now()
        }
        id
    }

    /**
     * Phase 1: drain the [segments] flow into the staging table in bounded
     * batches. Backpressure is preserved because we only pull the next batch
     * after the previous batch is committed. Returns the total staged.
     */
    suspend fun ingest(jobId: String, segments: Flow<Segment>): Long {
        var offset = existingStagedCount(jobId)
        segments.collectChunked(chunkSize, skip = offset) { batch, _ ->
            db.dbQuery {
                var idx = offset
                ImportSegments.batchInsert(batch) { seg ->
                    this[ImportSegments.jobId] = jobId
                    this[ImportSegments.offsetIndex] = idx++
                    this[ImportSegments.payload] = storyboardJson.encodeToString(Segment.serializer(), seg)
                }
            }
            offset += batch.size
            true
        }
        db.dbQuery {
            ImportJobs.update({ ImportJobs.jobId eq jobId }) {
                it[totalSegments] = offset
                it[status] = ImportStatus.RUNNING.name
                it[updatedAt] = Instant.now()
            }
        }
        return offset
    }

    private suspend fun existingStagedCount(jobId: String): Long = db.dbQuery {
        ImportSegments.selectAll().where { ImportSegments.jobId eq jobId }.count()
    }

    suspend fun requestCancel(jobId: String): Boolean = db.dbQuery {
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[cancelRequested] = true
            it[updatedAt] = Instant.now()
        } > 0
    }

    suspend fun progress(jobId: String): ImportProgress? = db.dbQuery { progressTx(jobId) }

    private fun progressTx(jobId: String): ImportProgress? =
        ImportJobs.selectAll().where { ImportJobs.jobId eq jobId }.firstOrNull()?.let {
            ImportProgress(
                jobId = it[ImportJobs.jobId],
                storyboardId = it[ImportJobs.storyboardId],
                status = ImportStatus.valueOf(it[ImportJobs.status]),
                total = it[ImportJobs.totalSegments],
                processed = it[ImportJobs.processedSegments],
            )
        }

    /**
     * Phase 2: apply staged segments from the persistent source, resuming at the
     * checkpoint. Reads one page at a time by offset so memory stays bounded
     * regardless of total size. Honors the durable cancel flag between chunks.
     */
    suspend fun run(jobId: String): ImportProgress {
        val start = progress(jobId) ?: error("import job $jobId not found")
        if (start.status == ImportStatus.COMPLETED) return start

        val storyboardId = start.storyboardId
        var offset = start.processed

        while (true) {
            if (isCancelled(jobId)) {
                markStatus(jobId, ImportStatus.CANCELLED)
                return progress(jobId)!!
            }
            val page = readPage(jobId, offset, chunkSize)
            if (page.isEmpty()) break
            appendChunk(storyboardId, page)
            offset += page.size
            checkpoint(jobId, offset)
        }
        markStatus(jobId, ImportStatus.COMPLETED)
        return progress(jobId)!!
    }

    /**
     * Resume a cancelled/interrupted job: clear the cancel flag, flip status back
     * to RUNNING, and actually re-run the apply phase from the DB checkpoint.
     */
    suspend fun resume(jobId: String): ImportProgress? {
        val p = progress(jobId) ?: return null
        if (p.status == ImportStatus.COMPLETED) return p
        db.dbQuery {
            ImportJobs.update({ ImportJobs.jobId eq jobId }) {
                it[cancelRequested] = false
                it[status] = ImportStatus.RUNNING.name
                it[updatedAt] = Instant.now()
            }
        }
        return run(jobId)
    }

    private suspend fun readPage(jobId: String, fromOffset: Long, limit: Int): List<Segment> = db.dbQuery {
        ImportSegments.selectAll()
            .where { (ImportSegments.jobId eq jobId) and (ImportSegments.offsetIndex greaterEq fromOffset) }
            .orderBy(ImportSegments.offsetIndex to SortOrder.ASC)
            .limit(limit)
            .map { storyboardJson.decodeFromString(Segment.serializer(), it[ImportSegments.payload]) }
    }

    private suspend fun appendChunk(storyboardId: String, batch: List<Segment>) {
        if (batch.isEmpty()) return
        db.dbQuery {
            val expected = eventStore.currentHeadVersion(storyboardId)
            eventStore.append(
                StoryboardEvent(
                    eventId = UUID.randomUUID().toString(),
                    storyboardId = storyboardId,
                    version = 0,
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
