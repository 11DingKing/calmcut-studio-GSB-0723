package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.projection.StoryboardState
import org.slf4j.LoggerFactory

/**
 * The heart of the analysis worker. For each delivered event it:
 *  1. drops duplicates by event id (幂等),
 *  2. orders events by version using a **durable** buffer: an event ahead of the
 *     contiguous head is persisted to `pending_event` before its Kafka offset is
 *     committed, so a crash before the missing version arrives never loses it
 *     (乱序版本缓冲 + 安全提交),
 *  3. once the gap fills, folds each contiguous event onto the projection and
 *     runs *incremental* analysis over the affected window (== full recompute),
 *  4. commits projection + analysis + processed-version + event-id + pending
 *     removal in a single transaction (atomic, crash-safe),
 *  5. retries on failure and routes exhausted events to the dead-letter table
 *     (死信重放, auditable).
 *
 * Crash-safety: because processing is idempotent and the durable pending buffer
 * is written before offset commit, re-delivery after any crash reproduces the
 * identical projection and never drops a buffered event.
 */
class IdempotentProcessor(
    private val analyzer: RiskAnalyzer,
    private val repo: WorkerRepository,
    private val publisher: ResultPublisher,
    private val maxAttempts: Int = 3,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Process a single delivered event; returns the results produced (if any). */
    suspend fun process(event: StoryboardEvent): List<AnalysisResult> {
        if (repo.isProcessed(event.eventId)) {
            log.debug("event {} already processed/buffered, skipping", event.eventId)
            return emptyList()
        }

        val lastVersion = repo.lastProcessedVersion(event.storyboardId)
        return when {
            event.version <= lastVersion -> {
                // Stale: already applied. Record the id so redelivery is a no-op.
                log.debug("event {} v{} stale (last {}), dropping", event.eventId, event.version, lastVersion)
                repo.markProcessed(event.eventId)
                emptyList()
            }
            event.version > lastVersion + 1 -> {
                // Ahead of the head: durably buffer BEFORE the offset is committed.
                log.debug("event {} v{} buffered awaiting v{}", event.eventId, event.version, lastVersion + 1)
                repo.bufferPending(event)
                emptyList()
            }
            else -> {
                // Exactly the next version: apply it, then drain any contiguous run
                // that the durable buffer can now release.
                drainContiguous(event)
            }
        }
    }

    /** Applies [first] then any durably-buffered events that are now contiguous. */
    private suspend fun drainContiguous(first: StoryboardEvent): List<AnalysisResult> {
        val results = ArrayList<AnalysisResult>()
        var current: StoryboardEvent? = first
        while (current != null) {
            val applied = applyWithRetry(current) ?: break
            results += applied
            val nextVersion = repo.lastProcessedVersion(current.storyboardId) + 1
            current = repo.nextPending(current.storyboardId, nextVersion)
        }
        return results
    }

    /** Applies one event with retry; on exhaustion dead-letters it. Returns null on failure. */
    private suspend fun applyWithRetry(event: StoryboardEvent): AnalysisResult? {
        var attempt = 0
        var lastError: Throwable? = null
        while (attempt < maxAttempts) {
            attempt++
            try {
                return applyOnce(event)
            } catch (t: Throwable) {
                lastError = t
                log.warn("attempt {}/{} failed for event {}: {}", attempt, maxAttempts, event.eventId, t.message)
            }
        }
        repo.deadLetter(event, lastError?.message ?: "unknown error", attempt)
        // Record id so the poisoned event is not endlessly retried on redelivery,
        // and remove it from the durable buffer if it was parked there.
        repo.markProcessed(event.eventId)
        return null
    }

    /** Projects + analyses one event and commits everything atomically. */
    private suspend fun applyOnce(event: StoryboardEvent): AnalysisResult {
        val prior = repo.loadState(event.storyboardId)
        val oldTimeline = prior.timeline()
        val newState = StoryboardProjector.apply(prior, event)
        val newTimeline = newState.timeline()

        val priorAnalysis = repo.loadAnalysis(event.storyboardId)
        val result = if (priorAnalysis == null || oldTimeline.size == 0) {
            analyzer.analyzeFull(event.storyboardId, newState.version, newTimeline)
        } else {
            val changed = TimelineDiff.changedRange(oldTimeline, newTimeline)
            analyzer.analyzeIncremental(priorAnalysis, newState.version, newTimeline, changed)
        }

        // Single-transaction commit: projection + analysis + processed version +
        // event id + pending-row removal. Publishing happens after the commit so a
        // publish failure cannot roll back applied state; redelivery is idempotent.
        repo.commitApplied(newState, result, event.eventId)
        publisher.publish(result)
        return result
    }

    /** Replays a previously dead-lettered event through the normal pipeline. */
    suspend fun replayDeadLetter(eventId: String): List<AnalysisResult> {
        val event = repo.takeForReplay(eventId)
        if (event == null) {
            repo.recordAudit(WorkerRepository.AUDIT_DLQ_REPLAY, eventId, "no such dead-letter entry", WorkerRepository.OUTCOME_FAILURE)
            return emptyList()
        }
        return try {
            val results = drainContiguous(event)
            val outcome = if (results.isEmpty()) WorkerRepository.OUTCOME_FAILURE else WorkerRepository.OUTCOME_SUCCESS
            repo.recordAudit(WorkerRepository.AUDIT_DLQ_REPLAY, eventId, "replayed v${event.version} of ${event.storyboardId}", outcome)
            results
        } catch (t: Throwable) {
            repo.recordAudit(WorkerRepository.AUDIT_DLQ_REPLAY, eventId, "replay error: ${t.message}", WorkerRepository.OUTCOME_FAILURE)
            throw t
        }
    }

    suspend fun deadLetters(): List<DlqEntry> = repo.listDeadLetters()

    suspend fun auditTrail(target: String): List<AuditEntry> = repo.listAudit(target)
}
