package com.calmcut.studio.testutil

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.projection.StoryboardState
import com.calmcut.studio.worker.AuditEntry
import com.calmcut.studio.worker.DlqEntry
import com.calmcut.studio.worker.ResultPublisher
import com.calmcut.studio.worker.WorkerRepository
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * In-memory [WorkerRepository] for unit tests. Mirrors the durability contract of
 * the DB implementation: pending events are stored in a map (standing in for the
 * durable `pending_event` table) and [commitApplied] mutates projection + version
 * + processed-id + pending removal together.
 */
class InMemoryWorkerRepository : WorkerRepository {
    private val states = ConcurrentHashMap<String, StoryboardState>()
    private val analyses = ConcurrentHashMap<String, AnalysisResult>()
    private val processedIds = ConcurrentHashMap.newKeySet<String>()
    private val processedVersion = ConcurrentHashMap<String, Long>()
    private val pending = ConcurrentHashMap<String, StoryboardEvent>() // key: "$sb#$version"
    private val dlq = mutableListOf<DlqEntry>()
    private val audit = mutableListOf<AuditEntry>()

    private fun key(sb: String, v: Long) = "$sb#$v"

    override suspend fun isProcessed(eventId: String) = processedIds.contains(eventId)

    override suspend fun lastProcessedVersion(storyboardId: String) = processedVersion[storyboardId] ?: 0L

    override suspend fun loadState(storyboardId: String) =
        states[storyboardId] ?: StoryboardState.empty(storyboardId)

    override suspend fun loadAnalysis(storyboardId: String) = analyses[storyboardId]

    override suspend fun bufferPending(event: StoryboardEvent) {
        pending.putIfAbsent(key(event.storyboardId, event.version), event)
        processedIds.add(event.eventId)
    }

    override suspend fun nextPending(storyboardId: String, version: Long) = pending[key(storyboardId, version)]

    override suspend fun commitApplied(newState: StoryboardState, result: AnalysisResult, eventId: String) {
        states[newState.storyboardId] = newState
        analyses[newState.storyboardId] = result
        processedVersion[newState.storyboardId] = newState.version
        processedIds.add(eventId)
        pending.remove(key(newState.storyboardId, newState.version))
    }

    override suspend fun markProcessed(eventId: String) { processedIds.add(eventId) }

    override suspend fun deadLetter(event: StoryboardEvent, error: String, attempts: Int) {
        dlq += DlqEntry(event, error, attempts, replayed = false, replayedAt = null)
        pending.remove(key(event.storyboardId, event.version))
    }

    override suspend fun listDeadLetters(): List<DlqEntry> = dlq.toList()

    override suspend fun takeForReplay(eventId: String): StoryboardEvent? {
        val idx = dlq.indexOfFirst { it.event.eventId == eventId && !it.replayed }
        if (idx < 0) return null
        val entry = dlq[idx]
        dlq[idx] = entry.copy(replayed = true, replayedAt = Instant.now().toString())
        processedIds.remove(eventId)
        return entry.event
    }

    override suspend fun recordAudit(kind: String, target: String, detail: String, outcome: String) {
        audit += AuditEntry(kind, target, detail, outcome, Instant.now().toString())
    }

    override suspend fun listAudit(target: String): List<AuditEntry> = audit.filter { it.target == target }

    override suspend fun resetStoryboard(storyboardId: String) {
        states.remove(storyboardId)
        analyses.remove(storyboardId)
        processedVersion.remove(storyboardId)
        pending.keys.removeIf { it.startsWith("$storyboardId#") }
    }

    override suspend fun resetAll() {
        states.clear(); analyses.clear(); processedIds.clear()
        processedVersion.clear(); pending.clear(); dlq.clear(); audit.clear()
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
