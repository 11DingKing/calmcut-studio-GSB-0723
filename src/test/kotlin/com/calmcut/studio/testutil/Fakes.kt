package com.calmcut.studio.testutil

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.StoryboardState
import com.calmcut.studio.worker.DeadLetterSink
import com.calmcut.studio.worker.IdempotencyChecker
import com.calmcut.studio.worker.ProjectionStore
import com.calmcut.studio.worker.ResultPublisher
import java.util.concurrent.ConcurrentHashMap

/** In-memory projection store for worker tests. */
class InMemoryProjectionStore : ProjectionStore {
    private val states = ConcurrentHashMap<String, StoryboardState>()
    private val analyses = ConcurrentHashMap<String, AnalysisResult>()

    override suspend fun loadState(storyboardId: String) = states[storyboardId]
    override suspend fun saveState(state: StoryboardState) { states[state.storyboardId] = state }
    override suspend fun loadAnalysis(storyboardId: String) = analyses[storyboardId]
    override suspend fun saveAnalysis(result: AnalysisResult) { analyses[result.storyboardId] = result }
    override suspend fun resetAll() { states.clear(); analyses.clear() }
}

/** In-memory idempotency ledger. */
class InMemoryIdempotencyChecker : IdempotencyChecker {
    private val seen = ConcurrentHashMap.newKeySet<String>()
    private val versions = ConcurrentHashMap<String, Long>()

    override suspend fun markIfFirst(eventId: String): Boolean = seen.add(eventId)
    override suspend fun seen(eventId: String): Boolean = seen.contains(eventId)
    override suspend fun lastProcessedVersion(storyboardId: String): Long = versions[storyboardId] ?: 0L
    override suspend fun recordVersion(storyboardId: String, version: Long) { versions[storyboardId] = version }
    override suspend fun resetAll() { seen.clear(); versions.clear() }
}

/** In-memory dead-letter sink with replay. */
class InMemoryDeadLetterSink : DeadLetterSink {
    private val entries = mutableListOf<DeadLetterSink.DlqEntry>()

    override suspend fun send(event: StoryboardEvent, error: String, attempts: Int) {
        entries += DeadLetterSink.DlqEntry(event, error, attempts, replayed = false)
    }
    override suspend fun list(): List<DeadLetterSink.DlqEntry> = entries.toList()
    override suspend fun takeForReplay(eventId: String): StoryboardEvent? {
        val idx = entries.indexOfFirst { it.event.eventId == eventId && !it.replayed }
        if (idx < 0) return null
        val entry = entries[idx]
        entries[idx] = entry.copy(replayed = true)
        return entry.event
    }
}

/** Records every published result; can be told to throw to simulate crashes. */
class RecordingResultPublisher : ResultPublisher {
    val published = mutableListOf<AnalysisResult>()
    var failNextCount = 0

    override suspend fun publish(result: AnalysisResult) {
        if (failNextCount > 0) {
            failNextCount--
            throw RuntimeException("simulated publish failure")
        }
        published += result
    }
}
