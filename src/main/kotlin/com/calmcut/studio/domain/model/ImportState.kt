package com.calmcut.studio.domain.model

import kotlinx.serialization.Serializable

@Serializable
data class ImportState(
    val jobId: String,
    val timelineId: String,
    val totalCount: Long,
    val processedCount: Long,
    val failedCount: Long,
    val status: Status,
    val cancelRequested: Boolean,
    val resumeToken: String? = null,
    val errorMessage: String? = null
) {
    enum class Status { PENDING, IN_PROGRESS, COMPLETED, CANCELLED, FAILED }

    val progress: Double get() = if (totalCount == 0L) 0.0 else processedCount.toDouble() / totalCount.toDouble()

    val isTerminal: Boolean get() = status in setOf(Status.COMPLETED, Status.CANCELLED, Status.FAILED)
}

@Serializable
data class ImportChunkPayload(
    val jobId: String,
    val chunkIndex: Int,
    val segments: List<StoryboardSegment>,
    val isLast: Boolean
)
