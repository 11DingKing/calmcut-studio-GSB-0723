package com.calmcut.studio.worker

/**
 * Tracks which event ids have already been processed so that duplicate Kafka
 * deliveries are no-ops ("消费者需幂等"). Implementations are backed by the
 * `processed_event` table in production and by an in-memory set in tests.
 */
interface IdempotencyChecker {
    /** Returns true if this is the first time [eventId] is seen (and records it). */
    suspend fun markIfFirst(eventId: String): Boolean

    /** True if [eventId] was already processed. */
    suspend fun seen(eventId: String): Boolean

    /** The last storyboard version already applied, for stale-drop decisions. */
    suspend fun lastProcessedVersion(storyboardId: String): Long

    /** Advance the last-processed version for a storyboard. */
    suspend fun recordVersion(storyboardId: String, version: Long)

    /** Clears all state — used by "rebuild projection from zero". */
    suspend fun resetAll()
}
