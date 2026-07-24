package com.calmcut.studio.db

import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

/** Append-only domain event log; unique on (storyboardId, version). */
object StoryboardEvents : LongIdTable("storyboard_event") {
    val eventId = varchar("event_id", 64).uniqueIndex()
    val storyboardId = varchar("storyboard_id", 64).index()
    val version = long("version")
    val eventType = varchar("event_type", 32)
    val payload = text("payload")
    val createdAt = timestamp("created_at")

    init {
        uniqueIndex(storyboardId, version)
    }
}

/** Transactional outbox drained by the publisher. */
object Outbox : LongIdTable("outbox") {
    val eventId = varchar("event_id", 64).uniqueIndex()
    val storyboardId = varchar("storyboard_id", 64)
    val version = long("version")
    val topic = varchar("topic", 128)
    val payload = text("payload")
    val published = bool("published").default(false)
    val createdAt = timestamp("created_at")
    val publishedAt = timestamp("published_at").nullable()
}

/** Optimistic-lock head: current version + knowledge points per storyboard. */
object StoryboardHead : Table("storyboard_head") {
    val storyboardId = varchar("storyboard_id", 64)
    val version = long("version")
    val revisionKp = text("revision_kp").nullable()
    val originalKp = text("original_kp").nullable()
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(storyboardId)
}

/** Continuous non-overlapping segment projection. */
object SegmentProjection : Table("segment_projection") {
    val storyboardId = varchar("storyboard_id", 64)
    val segmentId = varchar("segment_id", 64)
    val orderIndex = integer("order_index")
    val startMs = long("start_ms")
    val durationMs = long("duration_ms")
    val intensity = integer("intensity")
    val isReversal = bool("is_reversal").default(false)
    val isDeclineInducement = bool("is_decline_inducement").default(false)
    val knowledgePoint = varchar("knowledge_point", 128).nullable()
    val projectionVersion = long("projection_version")
    override val primaryKey = PrimaryKey(storyboardId, segmentId)
}

/** Latest analysis result projection. */
object AnalysisProjection : Table("analysis_projection") {
    val storyboardId = varchar("storyboard_id", 64)
    val projectionVersion = long("projection_version")
    val ruleVersion = varchar("rule_version", 32)
    val findings = text("findings")
    val analyzedAt = timestamp("analyzed_at")
    override val primaryKey = PrimaryKey(storyboardId)
}

/** Idempotency: last-processed version per storyboard. */
object ProcessedVersion : Table("processed_version") {
    val storyboardId = varchar("storyboard_id", 64)
    val lastVersion = long("last_version")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(storyboardId)
}

/** Idempotency: already-consumed event ids. */
object ProcessedEvent : Table("processed_event") {
    val eventId = varchar("event_id", 64)
    val processedAt = timestamp("processed_at")
    override val primaryKey = PrimaryKey(eventId)
}

/** Dead-letter storage with replay flag. */
object DeadLetter : LongIdTable("dead_letter") {
    val eventId = varchar("event_id", 64)
    val storyboardId = varchar("storyboard_id", 64)
    val version = long("version")
    val payload = text("payload")
    val error = text("error")
    val attempts = integer("attempts")
    val replayed = bool("replayed").default(false)
    val createdAt = timestamp("created_at")
}

/** Streaming import jobs with cancel/resume checkpoints. */
object ImportJobs : Table("import_job") {
    val jobId = varchar("job_id", 64)
    val storyboardId = varchar("storyboard_id", 64)
    val status = varchar("status", 16)
    val totalSegments = long("total_segments").default(0)
    val processedSegments = long("processed_segments").default(0)
    val lastOffset = long("last_offset").default(0)
    val cancelRequested = bool("cancel_requested").default(false)
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")
    override val primaryKey = PrimaryKey(jobId)
}
