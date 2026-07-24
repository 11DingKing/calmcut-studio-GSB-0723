package com.calmcut.studio.db

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.messaging.KafkaEventProducer
import com.calmcut.studio.projection.StoryboardState
import com.calmcut.studio.worker.DeadLetterSink
import com.calmcut.studio.worker.IdempotencyChecker
import com.calmcut.studio.worker.ProjectionStore
import com.calmcut.studio.worker.ResultPublisher
import java.time.Instant
import kotlinx.serialization.builtins.ListSerializer
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import org.jetbrains.exposed.sql.upsert

private val findingsSerializer = ListSerializer(RiskFinding.serializer())

/** Postgres-backed projection store over the segment + analysis projection tables. */
class DbProjectionStore(private val db: DatabaseFactory, private val eventsTopic: String) : ProjectionStore {

    override suspend fun loadState(storyboardId: String): StoryboardState? = db.dbQuery {
        val headVersion = StoryboardHead
            .select(StoryboardHead.version, StoryboardHead.revisionKp, StoryboardHead.originalKp)
            .where { StoryboardHead.storyboardId eq storyboardId }
            .firstOrNull() ?: return@dbQuery null

        val segments = SegmentProjection
            .selectAll()
            .where { SegmentProjection.storyboardId eq storyboardId }
            .orderBy(SegmentProjection.orderIndex to SortOrder.ASC)
            .map {
                Segment(
                    id = it[SegmentProjection.segmentId],
                    orderIndex = it[SegmentProjection.orderIndex],
                    durationMs = it[SegmentProjection.durationMs],
                    intensity = it[SegmentProjection.intensity],
                    strongReversal = it[SegmentProjection.isReversal],
                    declineInducement = it[SegmentProjection.isDeclineInducement],
                    knowledgePoint = it[SegmentProjection.knowledgePoint],
                )
            }
        val kp = headVersion[StoryboardHead.revisionKp]?.let {
            storyboardJson.decodeFromString(KnowledgePointSet.serializer(), it)
        } ?: KnowledgePointSet()
        StoryboardState(storyboardId, headVersion[StoryboardHead.version], segments.associateBy { it.id }, kp)
    }

    override suspend fun saveState(state: StoryboardState): Unit = db.dbQuery {
        SegmentProjection.deleteWhere { SegmentProjection.storyboardId eq state.storyboardId }
        val timeline = state.timeline()
        timeline.segments.forEachIndexed { i, seg ->
            SegmentProjection.insert {
                it[storyboardId] = state.storyboardId
                it[segmentId] = seg.id
                it[orderIndex] = seg.orderIndex
                it[startMs] = timeline.startMs(i)
                it[durationMs] = seg.durationMs
                it[intensity] = seg.intensity
                it[isReversal] = seg.strongReversal
                it[isDeclineInducement] = seg.declineInducement
                it[knowledgePoint] = seg.knowledgePoint
                it[projectionVersion] = state.version
            }
        }
        StoryboardHead.upsert(StoryboardHead.storyboardId) {
            it[storyboardId] = state.storyboardId
            it[version] = state.version
            it[updatedAt] = Instant.now()
        }
    }

    override suspend fun loadAnalysis(storyboardId: String): AnalysisResult? = db.dbQuery {
        AnalysisProjection
            .selectAll()
            .where { AnalysisProjection.storyboardId eq storyboardId }
            .firstOrNull()
            ?.let {
                AnalysisResult(
                    storyboardId = storyboardId,
                    version = it[AnalysisProjection.projectionVersion],
                    ruleVersion = it[AnalysisProjection.ruleVersion],
                    findings = storyboardJson.decodeFromString(findingsSerializer, it[AnalysisProjection.findings]),
                )
            }
    }

    override suspend fun saveAnalysis(result: AnalysisResult): Unit = db.dbQuery {
        AnalysisProjection.upsert(AnalysisProjection.storyboardId) {
            it[storyboardId] = result.storyboardId
            it[projectionVersion] = result.version
            it[ruleVersion] = result.ruleVersion
            it[findings] = storyboardJson.encodeToString(findingsSerializer, result.findings)
            it[analyzedAt] = Instant.now()
        }
    }

    override suspend fun resetAll(): Unit = db.dbQuery {
        SegmentProjection.deleteAll()
        AnalysisProjection.deleteAll()
    }
}

/** Postgres-backed idempotency ledger over processed_event + processed_version. */
class DbIdempotencyChecker(private val db: DatabaseFactory) : IdempotencyChecker {

    override suspend fun markIfFirst(eventId: String): Boolean = db.dbQuery {
        val exists = ProcessedEvent.selectAll().where { ProcessedEvent.eventId eq eventId }.any()
        if (exists) return@dbQuery false
        ProcessedEvent.insert {
            it[this.eventId] = eventId
            it[processedAt] = Instant.now()
        }
        true
    }

    override suspend fun seen(eventId: String): Boolean = db.dbQuery {
        ProcessedEvent.selectAll().where { ProcessedEvent.eventId eq eventId }.any()
    }

    override suspend fun lastProcessedVersion(storyboardId: String): Long = db.dbQuery {
        ProcessedVersion
            .select(ProcessedVersion.lastVersion)
            .where { ProcessedVersion.storyboardId eq storyboardId }
            .firstOrNull()?.get(ProcessedVersion.lastVersion) ?: 0L
    }

    override suspend fun recordVersion(storyboardId: String, version: Long): Unit = db.dbQuery {
        ProcessedVersion.upsert(ProcessedVersion.storyboardId) {
            it[this.storyboardId] = storyboardId
            it[lastVersion] = version
            it[updatedAt] = Instant.now()
        }
    }

    override suspend fun resetAll(): Unit = db.dbQuery {
        ProcessedEvent.deleteAll()
        ProcessedVersion.deleteAll()
    }
}

/** Postgres-backed dead-letter queue with replay support. */
class DbDeadLetterSink(private val db: DatabaseFactory) : DeadLetterSink {

    override suspend fun send(event: StoryboardEvent, error: String, attempts: Int): Unit = db.dbQuery {
        DeadLetter.insert {
            it[eventId] = event.eventId
            it[storyboardId] = event.storyboardId
            it[version] = event.version
            it[payload] = storyboardJson.encodeToString(StoryboardEvent.serializer(), event)
            it[this.error] = error
            it[this.attempts] = attempts
            it[replayed] = false
            it[createdAt] = Instant.now()
        }
    }

    override suspend fun list(): List<DeadLetterSink.DlqEntry> = db.dbQuery {
        DeadLetter.selectAll().orderBy(DeadLetter.id to SortOrder.ASC).map {
            DeadLetterSink.DlqEntry(
                event = storyboardJson.decodeFromString(StoryboardEvent.serializer(), it[DeadLetter.payload]),
                error = it[DeadLetter.error],
                attempts = it[DeadLetter.attempts],
                replayed = it[DeadLetter.replayed],
            )
        }
    }

    override suspend fun takeForReplay(eventId: String): StoryboardEvent? = db.dbQuery {
        val row = DeadLetter.selectAll()
            .where { (DeadLetter.eventId eq eventId) and (DeadLetter.replayed eq false) }
            .orderBy(DeadLetter.id to SortOrder.ASC)
            .firstOrNull() ?: return@dbQuery null
        DeadLetter.update({ DeadLetter.id eq row[DeadLetter.id] }) { it[replayed] = true }
        storyboardJson.decodeFromString(StoryboardEvent.serializer(), row[DeadLetter.payload])
    }
}

/** Publishes analysis results to the Kafka results topic. */
class KafkaResultPublisher(
    private val producer: KafkaEventProducer,
    private val kafka: AppConfig.KafkaConfig,
) : ResultPublisher {
    override suspend fun publish(result: AnalysisResult) {
        val payload = storyboardJson.encodeToString(AnalysisResult.serializer(), result)
        producer.send(kafka.resultsTopic, result.storyboardId, payload)
    }
}
