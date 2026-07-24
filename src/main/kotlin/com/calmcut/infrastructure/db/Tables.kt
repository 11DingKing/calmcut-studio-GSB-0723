package com.calmcut.infrastructure.db

import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.timestamp
import java.util.UUID

object Storyboards : Table("storyboards") {
    val id = uuid("id")
    val externalId = varchar("external_id", 255).uniqueIndex()
    val title = varchar("title", 512)
    val currentVersion = long("current_version").default(0)
    val knowledgePointCount = integer("knowledge_point_count").default(0)
    val originalKnowledgePoints = text("original_knowledge_points").nullable()
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(id)
}

object StoryboardSegments : Table("storyboard_segments") {
    val id = uuid("id")
    val storyboardId = uuid("storyboard_id")
    val segmentOrder = integer("segment_order")
    val startTimeMs = long("start_time_ms")
    val endTimeMs = long("end_time_ms")
    val stimulusIntensity = integer("stimulus_intensity")
    val hasReversal = bool("has_reversal").default(false)
    val isKnowledgePoint = bool("is_knowledge_point").default(false)
    val knowledgePointId = varchar("knowledge_point_id", 255).nullable()
    val hasScrollInducement = bool("has_scroll_inducement").default(false)
    val contentType = varchar("content_type", 64).default("CONTENT")
    val version = long("version").default(1)
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(id)
}

object EventLog : LongIdTable("event_log") {
    val eventId = uuid("event_id").uniqueIndex()
    val eventType = varchar("event_type", 64)
    val storyboardId = uuid("storyboard_id").nullable()
    val aggregateVersion = long("aggregate_version")
    val payload = text("payload")
    val metadata = text("metadata").default("{}")
    val occurredAt = timestamp("occurred_at")
    val processedAt = timestamp("processed_at").nullable()
}

object OutboxEvents : LongIdTable("outbox_events") {
    val eventId = uuid("event_id").uniqueIndex()
    val eventType = varchar("event_type", 64)
    val topic = varchar("topic", 255)
    val key = varchar("key", 255)
    val payload = text("payload")
    val headers = text("headers").default("{}")
    val createdAt = timestamp("created_at")
    val publishedAt = timestamp("published_at").nullable()
    val attempts = integer("attempts").default(0)
    val lastError = text("last_error").nullable()
}

object RiskProjections : LongIdTable("risk_projections") {
    val storyboardId = uuid("storyboard_id").uniqueIndex()
    val ruleVersion = varchar("rule_version", 32)
    val projectionVersion = long("projection_version").default(0)
    val lastEventId = long("last_event_id").nullable()
    val computedAt = timestamp("computed_at")
    val totalRiskScore = double("total_risk_score").default(0.0)
    val resultsJson = text("results_json").default("[]")
}

object RiskFindings : LongIdTable("risk_findings") {
    val projectionId = long("projection_id")
    val storyboardId = uuid("storyboard_id")
    val ruleId = varchar("rule_id", 64)
    val severity = varchar("severity", 32)
    val windowStartMs = long("window_start_ms").nullable()
    val windowEndMs = long("window_end_ms").nullable()
    val affectedSegmentIds = text("affected_segment_ids").default("{}")
    val evidenceJson = text("evidence_json").default("{}")
    val suggestion = text("suggestion")
    val createdAt = timestamp("created_at")
}

object ConsumerOffsets : Table("consumer_offsets") {
    val consumerGroup = varchar("consumer_group", 255)
    val topicPartition = varchar("topic_partition", 255)
    val offsetVal = long("offset_val").default(0)
    val lastEventId = long("last_event_id").nullable()
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(consumerGroup, topicPartition)
}

object DeadLetters : LongIdTable("dead_letters") {
    val originalTopic = varchar("original_topic", 255)
    val originalPartition = integer("original_partition")
    val originalOffset = long("original_offset")
    val eventType = varchar("event_type", 64)
    val storyboardId = uuid("storyboard_id").nullable()
    val payload = text("payload")
    val errorMessage = text("error_message")
    val errorStackTrace = text("error_stack_trace").nullable()
    val retryCount = integer("retry_count").default(0)
    val maxRetries = integer("max_retries").default(3)
    val nextRetryAt = timestamp("next_retry_at").nullable()
    val resolved = bool("resolved").default(false)
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
}

object ImportJobs : Table("import_jobs") {
    val id = uuid("id")
    val storyboardId = uuid("storyboard_id")
    val status = varchar("status", 32)
    val totalSegments = integer("total_segments").default(0)
    val processedSegments = integer("processed_segments").default(0)
    val failedSegments = integer("failed_segments").default(0)
    val checkpoint = integer("checkpoint").default(0)
    val startedAt = timestamp("started_at").nullable()
    val completedAt = timestamp("completed_at").nullable()
    val cancelledAt = timestamp("cancelled_at").nullable()
    val errorMessage = text("error_message").nullable()
    val jobMetadata = text("metadata").default("{}")
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(id)
}

object ImportBatches : LongIdTable("import_batches") {
    val importJobId = uuid("import_job_id")
    val batchNumber = integer("batch_number")
    val segmentData = text("segment_data")
    val processed = bool("processed").default(false)
    val processedAt = timestamp("processed_at").nullable()
    val errorMessage = text("error_message").nullable()
    val createdAt = timestamp("created_at")
    val uniqueBatch = uniqueIndex(importJobId, batchNumber)
}

object ProjectionDriftCheckpoints : LongIdTable("projection_drift_checkpoints") {
    val storyboardId = uuid("storyboard_id")
    val checkType = varchar("check_type", 64)
    val incrementalResult = text("incremental_result").nullable()
    val fullResult = text("full_result").nullable()
    val driftDetected = bool("drift_detected").default(false)
    val driftDetails = text("drift_details").nullable()
    val checkedAt = timestamp("checked_at")
}

object ConsumerProcessedOffsets : Table("consumer_processed_offsets") {
    val consumerGroup = varchar("consumer_group", 255)
    val topic = varchar("topic", 255)
    val partition = integer("partition")
    val offsetVal = long("offset_val")
    val eventId = uuid("event_id").nullable()
    val eventType = varchar("event_type", 64).nullable()
    val storyboardId = uuid("storyboard_id").nullable()
    val processedAt = timestamp("processed_at")
    override val primaryKey = PrimaryKey(consumerGroup, topic, partition, offsetVal)
}

object ConsumerStoryboardVersions : Table("consumer_storyboard_versions") {
    val consumerGroup = varchar("consumer_group", 255)
    val storyboardId = uuid("storyboard_id")
    val lastProcessedVersion = long("last_processed_version").default(0)
    val processedEventIds = text("processed_event_ids").default("{}")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(consumerGroup, storyboardId)
}
