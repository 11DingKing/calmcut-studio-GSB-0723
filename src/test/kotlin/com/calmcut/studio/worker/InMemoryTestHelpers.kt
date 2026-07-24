package com.calmcut.studio.worker

class InMemoryIdempotencyChecker : IdempotencyChecker {
    private val processed = mutableSetOf<String>()

    override suspend fun isProcessed(eventId: String): Boolean = eventId in processed

    override suspend fun markProcessed(eventId: String, result: String) {
        processed.add(eventId)
    }

    override suspend fun markFailed(eventId: String) {
        processed.remove(eventId)
    }

    override suspend fun resetAll() {
        processed.clear()
    }
}

class InMemoryDeadLetterSink : DeadLetterSink {
    val entries = mutableListOf<DeadLetterSink.DlqEntry>()
    private var nextId = 1L

    override suspend fun enqueue(
        eventId: String, topic: String, partition: Int, offset: Long, payload: String, error: Throwable
    ): Long {
        val id = nextId++
        entries.add(
            DeadLetterSink.DlqEntry(id, eventId, topic, partition, offset, payload,
                error.message ?: "", error::class.qualifiedName, 1)
        )
        return id
    }

    override suspend fun getUnreplayed(limit: Int): List<DeadLetterSink.DlqEntry> = entries.take(limit)

    override suspend fun markReplayed(id: Long) {
        entries.removeIf { it.id == id }
    }

    override suspend fun incrementAttempt(id: Long) {
        val idx = entries.indexOfFirst { it.id == id }
        if (idx >= 0) {
            entries[idx] = entries[idx].copy(attemptCount = entries[idx].attemptCount + 1)
        }
    }
}
