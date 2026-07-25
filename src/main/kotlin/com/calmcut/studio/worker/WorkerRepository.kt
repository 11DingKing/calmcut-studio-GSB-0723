package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.StoryboardState

/** A dead-lettered event with its failure context and auditable replay state. */
data class DlqEntry(
    val event: StoryboardEvent,
    val error: String,
    val attempts: Int,
    val replayed: Boolean,
    val replayedAt: String?,
)

/** One audit record of a DLQ replay or a projection rebuild. */
data class AuditEntry(
    val kind: String,
    val target: String,
    val detail: String,
    val outcome: String,
    val createdAt: String,
)

/**
 * The worker's durable persistence boundary. Consolidates the four things the
 * analysis pipeline must keep consistent — the projection, the idempotency
 * ledger, the **durable** out-of-order buffer, and the dead-letter/audit log —
 * so the crash-critical operations are single transactions.
 *
 * The key durability guarantee: [bufferPending] persists an out-of-order event
 * to durable storage *before* the Kafka offset for that record is committed, and
 * [commitApplied] atomically advances the projection, records the processed
 * version + event id, and removes the drained pending row in one transaction. A
 * crash at any point therefore never loses a buffered event and never
 * double-applies a committed one.
 */
interface WorkerRepository {

    /** True if [eventId] was already fully processed or durably buffered. */
    suspend fun isProcessed(eventId: String): Boolean

    /** Highest contiguous version already applied to the projection. */
    suspend fun lastProcessedVersion(storyboardId: String): Long

    /** Current projected state (empty state if the storyboard is unknown). */
    suspend fun loadState(storyboardId: String): StoryboardState

    /** Latest persisted analysis result, or null if none yet. */
    suspend fun loadAnalysis(storyboardId: String): AnalysisResult?

    /**
     * Durably buffer an event that is ahead of the contiguous head. Idempotent on
     * (storyboardId, version). Also records the event id as seen so redelivery is
     * a no-op. Must commit before the caller commits the Kafka offset.
     */
    suspend fun bufferPending(event: StoryboardEvent)

    /** The buffered event at exactly [version], or null if the gap is not filled. */
    suspend fun nextPending(storyboardId: String, version: Long): StoryboardEvent?

    /**
     * Atomically (one transaction): persist [newState] and [result], advance the
     * processed version, mark [eventId] processed, and delete the pending row at
     * [newState.version] if present.
     */
    suspend fun commitApplied(newState: StoryboardState, result: AnalysisResult, eventId: String)

    /** Record an event id as processed without applying it (stale / poisoned). */
    suspend fun markProcessed(eventId: String)

    /** Append a dead-letter record. */
    suspend fun deadLetter(event: StoryboardEvent, error: String, attempts: Int)

    /** List dead-letter entries in insertion order. */
    suspend fun listDeadLetters(): List<DlqEntry>

    /** Mark a DLQ entry replayed (stamping the time) and return its event. */
    suspend fun takeForReplay(eventId: String): StoryboardEvent?

    /** Append an audit record (DLQ replay or projection rebuild). */
    suspend fun recordAudit(kind: String, target: String, detail: String, outcome: String)

    /** List audit records for a target (event id or storyboard id). */
    suspend fun listAudit(target: String): List<AuditEntry>

    /** Clear all projection + idempotency + pending state for one storyboard. */
    suspend fun resetStoryboard(storyboardId: String)

    /** Clear everything (test / full rebuild support). */
    suspend fun resetAll()

    companion object {
        const val AUDIT_DLQ_REPLAY = "DLQ_REPLAY"
        const val AUDIT_REBUILD = "PROJECTION_REBUILD"
        const val OUTCOME_SUCCESS = "SUCCESS"
        const val OUTCOME_FAILURE = "FAILURE"
    }
}
