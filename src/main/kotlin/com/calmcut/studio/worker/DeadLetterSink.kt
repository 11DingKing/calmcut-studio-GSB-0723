package com.calmcut.studio.worker

import com.calmcut.studio.domain.StoryboardEvent

/**
 * Sink for events that repeatedly fail analysis ("死信重放"). Failed events are
 * captured with their error and attempt count; operators can list and replay
 * them once the underlying issue is fixed.
 */
interface DeadLetterSink {
    suspend fun send(event: StoryboardEvent, error: String, attempts: Int)
    suspend fun list(): List<DlqEntry>
    /** Marks an entry replayed and returns the event for reprocessing. */
    suspend fun takeForReplay(eventId: String): StoryboardEvent?

    data class DlqEntry(
        val event: StoryboardEvent,
        val error: String,
        val attempts: Int,
        val replayed: Boolean,
    )
}
