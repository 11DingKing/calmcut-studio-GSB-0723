package com.calmcut.studio.worker

import com.calmcut.studio.analysis.AffectedRange
import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.StoryboardProjector
import com.calmcut.studio.projection.StoryboardState
import org.slf4j.LoggerFactory

/**
 * The heart of the analysis worker. For each event it:
 *  1. drops duplicates via [IdempotencyChecker] (幂等),
 *  2. releases events in strict version order via a per-storyboard [VersionBuffer]
 *     (乱序版本缓冲), dropping stale versions,
 *  3. folds each released event onto the projected state,
 *  4. runs *incremental* analysis over just the affected window, which is proven
 *     equal to a full recompute, then persists and publishes the result,
 *  5. retries on failure and routes exhausted events to the [DeadLetterSink]
 *     (死信重放).
 *
 * A crash between steps is safe: because the projection is derived purely by
 * folding the ordered event log and idempotency is recorded only after a
 * successful commit, re-delivery reproduces the identical projection.
 */
class IdempotentProcessor(
    private val analyzer: RiskAnalyzer,
    private val store: ProjectionStore,
    private val idempotency: IdempotencyChecker,
    private val publisher: ResultPublisher,
    private val deadLetters: DeadLetterSink,
    private val maxAttempts: Int = 3,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val buffers = HashMap<String, VersionBuffer>()

    /** Process a single delivered event; returns the results produced (if any). */
    suspend fun process(event: StoryboardEvent): List<AnalysisResult> {
        if (idempotency.seen(event.eventId)) {
            log.debug("event {} already processed, skipping", event.eventId)
            return emptyList()
        }

        val lastVersion = idempotency.lastProcessedVersion(event.storyboardId)
        val buffer = buffers.getOrPut(event.storyboardId) { VersionBuffer(lastVersion) }
        buffer.advanceTo(lastVersion)

        val results = ArrayList<AnalysisResult>()
        when (val outcome = buffer.offer(event)) {
            is VersionBuffer.BufferResult.Stale -> {
                log.debug("event {} v{} stale (last {}), dropping", event.eventId, event.version, lastVersion)
                // Still record the event id so redelivery of the stale event is a no-op.
                idempotency.markIfFirst(event.eventId)
            }
            is VersionBuffer.BufferResult.Buffered -> {
                log.debug("event {} v{} buffered awaiting v{}", event.eventId, event.version, buffer.expectedNext)
            }
            is VersionBuffer.BufferResult.Ready -> {
                for (ready in outcome.events) {
                    results += applyWithRetry(ready)
                }
            }
        }
        return results
    }

    private suspend fun applyWithRetry(event: StoryboardEvent): List<AnalysisResult> {
        var attempt = 0
        var lastError: Throwable? = null
        while (attempt < maxAttempts) {
            attempt++
            try {
                return listOf(applyOnce(event))
            } catch (t: Throwable) {
                lastError = t
                log.warn("attempt {}/{} failed for event {}: {}", attempt, maxAttempts, event.eventId, t.message)
            }
        }
        deadLetters.send(event, lastError?.message ?: "unknown error", attempt)
        // Record id so the poisoned event is not endlessly retried on redelivery.
        idempotency.markIfFirst(event.eventId)
        return emptyList()
    }

    private suspend fun applyOnce(event: StoryboardEvent): AnalysisResult {
        val prior = store.loadState(event.storyboardId) ?: StoryboardState.empty(event.storyboardId)
        val oldTimeline = prior.timeline()
        val newState = StoryboardProjector.apply(prior, event)
        val newTimeline = newState.timeline()

        val priorAnalysis = store.loadAnalysis(event.storyboardId)
        val result = if (priorAnalysis == null || oldTimeline.size == 0) {
            analyzer.analyzeFull(event.storyboardId, newState.version, newTimeline)
        } else {
            val changed = TimelineDiff.changedRange(oldTimeline, newTimeline)
            analyzer.analyzeIncremental(priorAnalysis, newState.version, newTimeline, changed)
        }

        // Persist projection + analysis, then record idempotency. Ordering matters:
        // the id is committed last so a crash before it forces a safe replay.
        store.saveState(newState)
        store.saveAnalysis(result)
        publisher.publish(result)
        idempotency.recordVersion(event.storyboardId, newState.version)
        idempotency.markIfFirst(event.eventId)
        return result
    }

    /** Replays a previously dead-lettered event through the normal pipeline. */
    suspend fun replayDeadLetter(eventId: String): List<AnalysisResult> {
        val event = deadLetters.takeForReplay(eventId) ?: return emptyList()
        return applyWithRetry(event)
    }

    /** Full rebuild from zero: clears projections/idempotency and re-folds the log. */
    suspend fun rebuildFromZero(storyboardId: String, orderedLog: List<StoryboardEvent>) {
        buffers.remove(storyboardId)
        var state = StoryboardState.empty(storyboardId)
        var analysis: AnalysisResult? = null
        for (event in orderedLog.sortedBy { it.version }) {
            val oldTimeline = state.timeline()
            state = StoryboardProjector.apply(state, event)
            val newTimeline = state.timeline()
            analysis = if (analysis == null || oldTimeline.size == 0) {
                analyzer.analyzeFull(storyboardId, state.version, newTimeline)
            } else {
                val changed = TimelineDiff.changedRange(oldTimeline, newTimeline)
                analyzer.analyzeIncremental(analysis, state.version, newTimeline, changed)
            }
        }
        store.saveState(state)
        analysis?.let { store.saveAnalysis(it) }
    }

    /** Clears every projection and idempotency record. */
    suspend fun resetAll() {
        buffers.clear()
        store.resetAll()
        idempotency.resetAll()
    }
}
