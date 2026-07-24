package com.calmcut.studio.worker

interface IdempotencyChecker {
    suspend fun isProcessed(eventId: String): Boolean
    suspend fun markProcessed(eventId: String, result: String = "success")
    suspend fun markFailed(eventId: String)
    suspend fun resetAll()
}

interface DeadLetterSink {
    suspend fun enqueue(eventId: String, topic: String, partition: Int, offset: Long, payload: String, error: Throwable): Long
    suspend fun getUnreplayed(limit: Int = 100): List<DlqEntry>
    suspend fun markReplayed(id: Long)
    suspend fun incrementAttempt(id: Long)

    data class DlqEntry(
        val id: Long,
        val eventId: String,
        val topic: String,
        val partition: Int,
        val offset: Long,
        val payload: String,
        val errorMessage: String,
        val errorClass: String?,
        val attemptCount: Int
    )
}
