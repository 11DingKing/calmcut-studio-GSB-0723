package com.calmcut.studio.import

import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.db.ImportJobs
import com.calmcut.studio.db.ImportSegments
import com.calmcut.studio.db.storyboardJson
import com.calmcut.studio.domain.EventType
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readUTF8Line
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.Flow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insertIgnore
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
    val staged: Long,
)

/** Outcome of a streaming NDJSON ingest attempt. */
data class IngestResult(
    val jobId: String,
    val staged: Long,
    val truncated: Boolean,
    /** Largest number of segments held in memory at once during this ingest. */
    val peakBuffered: Int,
)

/** Raised when the request body ends before the declared X-Total-Segments count. */
class TruncatedStreamException(val staged: Long, val expected: Long) :
    RuntimeException("stream truncated: staged $staged of declared $expected segments")

/** Max bytes for a single NDJSON line; guards against an unbounded/hostile line. */
private const val MAX_LINE_BYTES = 1 shl 20 // 1 MiB

/**
 * Streams up to millions of "分镜" into a storyboard without ever holding the
 * whole set in memory. Two bounded-memory phases:
 *
 *  1. **Ingest** — [ingestNdjson] parses the request body incrementally, one
 *     NDJSON line at a time via a suspending [ByteReadChannel] read (which
 *     applies natural backpressure: we only pull more bytes once the previous
 *     chunk is committed). Parsed segments are buffered up to [chunkSize] and
 *     flushed to the durable `import_segment` staging table, then the buffer is
 *     cleared — so resident memory is O(chunkSize), never O(total).
 *  2. **Apply** — [run] reads staged rows back by offset and appends each chunk
 *     as one event, checkpointing after every chunk under a row lock so the
 *     apply is exactly-once even across concurrent resume calls.
 *
 * Idempotency: staged rows are keyed by (jobId, offsetIndex) where offsetIndex is
 * the segment's absolute position in the logical stream. Re-sending the stream
 * for the same job skips the already-staged prefix and inserts with
 * ON CONFLICT DO NOTHING, so duplicate requests / resumes never double-write.
 */
class StreamingImportService(
    private val db: DatabaseFactory,
    private val eventStore: EventStore,
    private val chunkSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Largest number of segments buffered in memory during the most recent
     * [ingestNdjson] call. Exposed for tests to assert bounded memory (should stay
     * <= [chunkSize] regardless of total stream size).
     */
    @Volatile
    var lastPeakBuffered: Int = 0
        private set

    suspend fun createJob(storyboardId: String): String = db.dbQuery {
        val id = UUID.randomUUID().toString()
        insertJob(id, storyboardId)
        id
    }

    /**
     * Create the job if [jobId] is null/unknown, else return the existing job
     * (enabling resume + request idempotency with a client-supplied job id).
     */
    suspend fun ensureJob(jobId: String?, storyboardId: String): String = db.dbQuery {
        if (jobId != null) {
            val existing = ImportJobs.selectAll().where { ImportJobs.jobId eq jobId }.firstOrNull()
            if (existing != null) return@dbQuery jobId
        }
        val id = jobId ?: UUID.randomUUID().toString()
        insertJob(id, storyboardId)
        id
    }

    private fun insertJob(id: String, storyboardId: String) {
        ImportJobs.insertIgnore {
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
    }

    // ---- Phase 1a: incremental NDJSON parsing straight from the socket --------

    /**
     * Consume an NDJSON request body ([channel]) line-by-line, staging segments in
     * bounded-memory chunks. If [expectedTotal] is provided (X-Total-Segments) and
     * the stream ends early, the already-staged rows are kept durably and a
     * [TruncatedStreamException] is raised so the caller can report a partial
     * upload that a later resume can complete.
     */
    suspend fun ingestNdjson(jobId: String, channel: ByteReadChannel, expectedTotal: Long? = null): IngestResult {
        val alreadyStaged = existingStagedCount(jobId)
        var position = 0L          // absolute line index in the logical stream
        var staged = alreadyStaged
        val buffer = ArrayList<Segment>(chunkSize)
        val peak = AtomicInteger(0)
        var truncated = false

        suspend fun flush() {
            if (buffer.isEmpty()) return
            val base = staged
            db.dbQuery {
                var idx = base
                // Batched INSERT ... ON CONFLICT DO NOTHING: fast, and re-sent
                // prefixes never duplicate (keyed by jobId, offsetIndex).
                ImportSegments.batchInsert(buffer, ignore = true) { seg ->
                    this[ImportSegments.jobId] = jobId
                    this[ImportSegments.offsetIndex] = idx++
                    this[ImportSegments.payload] = storyboardJson.encodeToString(Segment.serializer(), seg)
                }
            }
            staged += buffer.size
            buffer.clear()
        }

        try {
            while (true) {
                val line = channel.readUTF8Line(MAX_LINE_BYTES) ?: break
                if (line.isBlank()) continue
                position++
                // Skip the already-staged prefix on resume without re-inserting.
                if (position <= alreadyStaged) continue
                buffer.add(storyboardJson.decodeFromString(Segment.serializer(), line))
                peak.set(maxOf(peak.get(), buffer.size))
                if (buffer.size >= chunkSize) flush()
            }
            flush()
        } catch (t: Throwable) {
            // Persist whatever full lines we already buffered, then surface the break.
            runCatching { flush() }
            lastPeakBuffered = peak.get()
            markStatus(jobId, ImportStatus.INGESTING)
            log.warn("ingest stream for job {} broke after {} staged: {}", jobId, staged, t.message)
            throw t
        }

        lastPeakBuffered = peak.get()
        if (expectedTotal != null && staged < expectedTotal) {
            truncated = true
            markStatus(jobId, ImportStatus.INGESTING)
            throw TruncatedStreamException(staged, expectedTotal)
        }

        db.dbQuery {
            ImportJobs.update({ ImportJobs.jobId eq jobId }) {
                it[totalSegments] = staged
                it[status] = ImportStatus.RUNNING.name
                it[updatedAt] = Instant.now()
            }
        }
        return IngestResult(jobId, staged, truncated, peak.get())
    }

    // ---- Phase 1b: ingest from an in-memory/generated Flow (tests, internal) --

    /**
     * Drain a segment [Flow] into the staging table in bounded batches. Kept for
     * programmatic imports; the HTTP path uses [ingestNdjson].
     */
    suspend fun ingest(jobId: String, segments: Flow<Segment>): Long {
        val alreadyStaged = existingStagedCount(jobId)
        var offset = alreadyStaged
        segments.collectChunked(chunkSize, skip = alreadyStaged) { batch, _ ->
            val base = offset
            db.dbQuery {
                var idx = base
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
                staged = ImportSegments.selectAll().where { ImportSegments.jobId eq jobId }.count(),
            )
        }

    // ---- Phase 2: exactly-once apply -----------------------------------------

    /**
     * Apply staged segments from the persistent source, resuming at the
     * checkpoint. Each chunk is processed inside one transaction that locks the
     * import_job row (SELECT ... FOR UPDATE) and re-reads the checkpoint under the
     * lock, so concurrent/duplicate resume calls cannot double-apply a chunk.
     */
    suspend fun run(jobId: String): ImportProgress {
        val start = progress(jobId) ?: error("import job $jobId not found")
        if (start.status == ImportStatus.COMPLETED) return start
        val storyboardId = start.storyboardId

        while (true) {
            val step = applyOneChunkLocked(jobId, storyboardId)
            when (step) {
                ChunkOutcome.CANCELLED -> return progress(jobId)!!
                ChunkOutcome.DONE -> break
                ChunkOutcome.PROGRESSED -> continue
            }
        }
        markStatus(jobId, ImportStatus.COMPLETED)
        return progress(jobId)!!
    }

    private enum class ChunkOutcome { PROGRESSED, CANCELLED, DONE }

    /**
     * One transactional, row-locked apply step. Returns whether it progressed,
     * hit the cancel flag, or reached the end of the staged data.
     */
    private suspend fun applyOneChunkLocked(jobId: String, storyboardId: String): ChunkOutcome = db.dbQuery {
        // Lock the job row so only one applier advances the checkpoint at a time.
        val jobRow = ImportJobs.selectAll().where { ImportJobs.jobId eq jobId }.forUpdate().first()
        if (jobRow[ImportJobs.cancelRequested]) {
            ImportJobs.update({ ImportJobs.jobId eq jobId }) {
                it[status] = ImportStatus.CANCELLED.name
                it[updatedAt] = Instant.now()
            }
            return@dbQuery ChunkOutcome.CANCELLED
        }
        val offset = jobRow[ImportJobs.processedSegments]

        val page = ImportSegments.selectAll()
            .where { (ImportSegments.jobId eq jobId) and (ImportSegments.offsetIndex greaterEq offset) }
            .orderBy(ImportSegments.offsetIndex to SortOrder.ASC)
            .limit(chunkSize)
            .map { storyboardJson.decodeFromString(Segment.serializer(), it[ImportSegments.payload]) }

        if (page.isEmpty()) return@dbQuery ChunkOutcome.DONE

        // Append the chunk as one event within the SAME transaction/lock, then
        // advance the checkpoint atomically — so a crash can't leave the event
        // written but the checkpoint stale (which would re-append on resume).
        val expected = eventStore.currentHeadVersion(storyboardId)
        eventStore.append(
            StoryboardEvent(
                eventId = UUID.randomUUID().toString(),
                storyboardId = storyboardId,
                version = 0,
                type = EventType.SEGMENT_CREATED,
                segments = page,
            ),
            expectedVersion = expected,
        )
        val newOffset = offset + page.size
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[processedSegments] = newOffset
            it[lastOffset] = newOffset
            it[updatedAt] = Instant.now()
        }
        ChunkOutcome.PROGRESSED
    }

    /**
     * Resume a cancelled/interrupted job: clear the cancel flag, flip status back
     * to RUNNING, and re-run the exactly-once apply phase from the checkpoint.
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

    private suspend fun markStatus(jobId: String, status: ImportStatus) = db.dbQuery {
        ImportJobs.update({ ImportJobs.jobId eq jobId }) {
            it[this.status] = status.name
            it[updatedAt] = Instant.now()
        }
    }
}
