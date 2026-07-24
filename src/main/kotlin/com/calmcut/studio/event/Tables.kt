package com.calmcut.studio.event

import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp
import java.time.Instant

object EventLogTable : Table("event_log") {
    val eventId = uuid("event_id")
    val timelineId = varchar("timeline_id", 128)
    val aggregateId = varchar("aggregate_id", 128)
    val eventType = varchar("event_type", 64)
    val eventVersion = long("event_version")
    val payload = text("payload")
    val metadata = text("metadata").default("{}")
    val createdAt = timestamp("created_at").default(Instant.now())

    override val primaryKey = PrimaryKey(eventId)
}

object OutboxTable : Table("outbox") {
    val id = long("id").autoIncrement()
    val eventId = uuid("event_id")
    val aggregateId = varchar("aggregate_id", 128)
    val topic = varchar("topic", 128)
    val key = varchar("key", 256)
    val payload = text("payload")
    val headers = text("headers").default("{}")
    val createdAt = timestamp("created_at").default(Instant.now())
    val publishedAt = timestamp("published_at").nullable()
    val published = bool("published").default(false)

    override val primaryKey = PrimaryKey(id)
}

object TimelineProjectionTable : Table("timeline_projection") {
    val timelineId = varchar("timeline_id", 128)
    val currentVersion = long("current_version").default(0)
    val totalSegments = integer("total_segments").default(0)
    val totalDurationMs = long("total_duration_ms").default(0)
    val lastEventId = uuid("last_event_id").nullable()
    val updatedAt = timestamp("updated_at").default(Instant.now())
    val createdAt = timestamp("created_at").default(Instant.now())

    override val primaryKey = PrimaryKey(timelineId)
}

object SegmentProjectionTable : Table("segment_projection") {
    val segmentId = varchar("segment_id", 128)
    val timelineId = varchar("timeline_id", 128)
    val version = long("version")
    val orderIndex = integer("order_index")
    val startTimeMs = long("start_time_ms")
    val endTimeMs = long("end_time_ms")
    val intensity = integer("intensity")
    val isReversal = bool("is_reversal").default(false)
    val isKnowledgePoint = bool("is_knowledge_point").default(false)
    val isDeclineInducement = bool("is_decline_inducement").default(false)
    val knowledgePointId = varchar("knowledge_point_id", 128).nullable()
    val isOriginal = bool("is_original").default(true)
    val content = text("content").nullable()
    val createdAt = timestamp("created_at").default(Instant.now())
    val updatedAt = timestamp("updated_at").default(Instant.now())

    override val primaryKey = PrimaryKey(segmentId, timelineId)
}

object RiskFindingTable : Table("risk_finding") {
    val findingId = uuid("finding_id")
    val timelineId = varchar("timeline_id", 128)
    val ruleId = varchar("rule_id", 64)
    val ruleVersion = varchar("rule_version", 32)
    val severity = varchar("severity", 16)
    val segmentIds = text("segment_ids")
    val timeRangeStart = long("time_range_start").nullable()
    val timeRangeEnd = long("time_range_end").nullable()
    val evidence = text("evidence")
    val suggestion = text("suggestion")
    val analysisVersion = long("analysis_version")
    val createdAt = timestamp("created_at").default(Instant.now())

    override val primaryKey = PrimaryKey(findingId)
}

object ProcessedEventsTable : Table("processed_events") {
    val consumerGroup = varchar("consumer_group", 128)
    val eventId = uuid("event_id")
    val processedAt = timestamp("processed_at").default(Instant.now())
    val result = varchar("result", 32)

    override val primaryKey = PrimaryKey(consumerGroup, eventId)
}

object DeadLetterTable : Table("dead_letter") {
    val id = long("id").autoIncrement()
    val eventId = uuid("event_id")
    val consumerGroup = varchar("consumer_group", 128)
    val topic = varchar("topic", 128)
    val partition = integer("partition")
    val offset = long("offset")
    val payload = text("payload")
    val errorMessage = text("error_message")
    val errorClass = varchar("error_class", 256).nullable()
    val attemptCount = integer("attempt_count").default(1)
    val replayed = bool("replayed").default(false)
    val createdAt = timestamp("created_at").default(Instant.now())
    val replayedAt = timestamp("replayed_at").nullable()

    override val primaryKey = PrimaryKey(id)
}

object ImportJobTable : Table("import_job") {
    val jobId = uuid("job_id")
    val timelineId = varchar("timeline_id", 128)
    val totalCount = long("total_count").default(0)
    val processedCount = long("processed_count").default(0)
    val failedCount = long("failed_count").default(0)
    val status = varchar("status", 32).default("pending")
    val cancelRequested = bool("cancel_requested").default(false)
    val resumeToken = varchar("resume_token", 512).nullable()
    val errorMessage = text("error_message").nullable()
    val createdAt = timestamp("created_at").default(Instant.now())
    val updatedAt = timestamp("updated_at").default(Instant.now())
    val completedAt = timestamp("completed_at").nullable()

    override val primaryKey = PrimaryKey(jobId)
}

object ImportChunkTable : Table("import_chunk") {
    val id = long("id").autoIncrement()
    val jobId = uuid("job_id")
    val chunkIndex = integer("chunk_index")
    val segmentCount = integer("segment_count")
    val payload = text("payload")
    val status = varchar("status", 32).default("pending")
    val processedAt = timestamp("processed_at").nullable()
    val errorMessage = text("error_message").nullable()

    override val primaryKey = PrimaryKey(id)
}

object KnowledgeIntegrityTable : Table("knowledge_integrity") {
    val timelineId = varchar("timeline_id", 128)
    val kpId = varchar("kp_id", 128)
    val originalSegments = text("original_segments")
    val revisedSegments = text("revised_segments")
    val intact = bool("intact").default(true)
    val details = text("details").nullable()
    val checkedAt = timestamp("checked_at").default(Instant.now())

    override val primaryKey = PrimaryKey(timelineId, kpId)
}

object AnalysisCheckpointTable : Table("analysis_checkpoint") {
    val timelineId = varchar("timeline_id", 128)
    val lastEventId = uuid("last_event_id")
    val analysisVersion = long("analysis_version")
    val windowState = text("window_state").nullable()
    val computedAt = timestamp("computed_at").default(Instant.now())

    override val primaryKey = PrimaryKey(timelineId)
}

val eventJson = Json {
    ignoreUnknownKeys = true
    classDiscriminator = "type"
    encodeDefaults = true
}
