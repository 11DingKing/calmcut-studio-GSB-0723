package com.calmcut.studio.db

import com.calmcut.studio.analysis.AnalysisResult
import com.calmcut.studio.analysis.RiskFinding
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.KnowledgePointSet
import com.calmcut.studio.domain.Segment
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.messaging.KafkaEventProducer
import com.calmcut.studio.projection.StoryboardState
import com.calmcut.studio.worker.AuditEntry
import com.calmcut.studio.worker.DlqEntry
import com.calmcut.studio.worker.ResultPublisher
import com.calmcut.studio.worker.WorkerRepository
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

/**
 * Postgres-backed [WorkerRepository]. The crash-critical methods ([bufferPending],
 * [commitApplied]) each run in a single transaction so that:
 *  - an out-of-order event is durably persisted before its Kafka offset commits,
 *  - applying an event advances projection + analysis + processed-version +
 *    event-id + pending-row removal atomically (all-or-nothing).
 *
 * The private `*Tx` helpers assume they run inside an open Exposed transaction so
 * they can be composed into one atomic commit.
 */
class DbWorkerRepository(private val db: DatabaseFactory) : WorkerRepository {

    override suspend fun isProcessed(eventId: String): Boolean = db.dbQuery {
        ProcessedEvent.selectAll().where { ProcessedEvent.eventId eq eventId }.any()
    }

    override suspend fun lastProcessedVersion(storyboardId: String): Long = db.dbQuery {
        lastProcessedVersionTx(storyboardId)
    }

    private fun lastProcessedVersionTx(storyboardId: String): Long =
        ProcessedVersion
            .select(ProcessedVersion.lastVersion)
            .where { ProcessedVersion.storyboardId eq storyboardId }
            .firstOrNull()?.get(ProcessedVersion.lastVersion) ?: 0L

    override suspend fun loadState(storyboardId: String): StoryboardState = db.dbQuery {
        loadStateTx(storyboardId)
    }

    private fun loadStateTx(storyboardId: String): StoryboardState {
        // The projection's version is how far the WORKER has folded events
        // (ProcessedVersion), NOT the write-side head (StoryboardHead.version),
        // which the event-append path advances independently. Using the head here
        // would make the projector treat freshly consumed events as stale.
        val appliedVersion = lastProcessedVersionTx(storyboardId)
        val kpJson = StoryboardHead
            .select(StoryboardHead.revisionKp)
            .where { StoryboardHead.storyboardId eq storyboardId }
            .firstOrNull()?.get(StoryboardHead.revisionKp)

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
        if (appliedVersion == 0L && segments.isEmpty()) return StoryboardState.empty(storyboardId)
        val kp = kpJson?.let {
            storyboardJson.decodeFromString(KnowledgePointSet.serializer(), it)
        } ?: KnowledgePointSet()
        return StoryboardState(storyboardId, appliedVersion, segments.associateBy { it.id }, kp)
    }

    override suspend fun loadAnalysis(storyboardId: String): AnalysisResult? = db.dbQuery {
        loadAnalysisTx(storyboardId)
    }

    private fun loadAnalysisTx(storyboardId: String): AnalysisResult? =
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

    override suspend fun bufferPending(event: StoryboardEvent): Unit = db.dbQuery {
        // Idempotent on (storyboardId, version); also mark the id seen so the
        // buffered event is never re-buffered or lost on redelivery.
        val exists = PendingEvents.selectAll()
            .where { (PendingEvents.storyboardId eq event.storyboardId) and (PendingEvents.version eq event.version) }
            .any()
        if (!exists) {
            PendingEvents.insert {
                it[storyboardId] = event.storyboardId
                it[version] = event.version
                it[eventId] = event.eventId
                it[payload] = storyboardJson.encodeToString(StoryboardEvent.serializer(), event)
                it[receivedAt] = Instant.now()
            }
        }
        markProcessedTx(event.eventId)
    }

    override suspend fun nextPending(storyboardId: String, version: Long): StoryboardEvent? = db.dbQuery {
        PendingEvents.selectAll()
            .where { (PendingEvents.storyboardId eq storyboardId) and (PendingEvents.version eq version) }
            .firstOrNull()
            ?.let { storyboardJson.decodeFromString(StoryboardEvent.serializer(), it[PendingEvents.payload]) }
    }

    override suspend fun commitApplied(newState: StoryboardState, result: AnalysisResult, eventId: String): Unit =
        db.dbQuery {
            saveStateTx(newState)
            saveAnalysisTx(result)
            ProcessedVersion.upsert(ProcessedVersion.storyboardId) {
                it[storyboardId] = newState.storyboardId
                it[lastVersion] = newState.version
                it[updatedAt] = Instant.now()
            }
            markProcessedTx(eventId)
            PendingEvents.deleteWhere {
                (PendingEvents.storyboardId eq newState.storyboardId) and (PendingEvents.version eq newState.version)
            }
        }

    private fun saveStateTx(state: StoryboardState) {
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
        // Note: the worker does NOT write StoryboardHead.version — that column is
        // the write-side optimistic-lock head owned by EventStore.append. Worker
        // progress is tracked via ProcessedVersion in commitApplied().
    }

    private fun saveAnalysisTx(result: AnalysisResult) {
        AnalysisProjection.upsert(AnalysisProjection.storyboardId) {
            it[storyboardId] = result.storyboardId
            it[projectionVersion] = result.version
            it[ruleVersion] = result.ruleVersion
            it[findings] = storyboardJson.encodeToString(findingsSerializer, result.findings)
            it[analyzedAt] = Instant.now()
        }
    }

    override suspend fun markProcessed(eventId: String): Unit = db.dbQuery { markProcessedTx(eventId) }

    private fun markProcessedTx(eventId: String) {
        val exists = ProcessedEvent.selectAll().where { ProcessedEvent.eventId eq eventId }.any()
        if (!exists) {
            ProcessedEvent.insert {
                it[this.eventId] = eventId
                it[processedAt] = Instant.now()
            }
        }
    }

    override suspend fun deadLetter(event: StoryboardEvent, error: String, attempts: Int): Unit = db.dbQuery {
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
        // Remove any durable pending row for this version so it is not re-drained.
        PendingEvents.deleteWhere {
            (PendingEvents.storyboardId eq event.storyboardId) and (PendingEvents.version eq event.version)
        }
    }

    override suspend fun listDeadLetters(): List<DlqEntry> = db.dbQuery {
        DeadLetter.selectAll().orderBy(DeadLetter.id to SortOrder.ASC).map {
            DlqEntry(
                event = storyboardJson.decodeFromString(StoryboardEvent.serializer(), it[DeadLetter.payload]),
                error = it[DeadLetter.error],
                attempts = it[DeadLetter.attempts],
                replayed = it[DeadLetter.replayed],
                replayedAt = it[DeadLetter.replayedAt]?.toString(),
            )
        }
    }

    override suspend fun takeForReplay(eventId: String): StoryboardEvent? = db.dbQuery {
        val row = DeadLetter.selectAll()
            .where { (DeadLetter.eventId eq eventId) and (DeadLetter.replayed eq false) }
            .orderBy(DeadLetter.id to SortOrder.ASC)
            .firstOrNull() ?: return@dbQuery null
        DeadLetter.update({ DeadLetter.id eq row[DeadLetter.id] }) {
            it[replayed] = true
            it[replayedAt] = Instant.now()
        }
        // Clear the processed marker so the replayed event applies again.
        ProcessedEvent.deleteWhere { ProcessedEvent.eventId eq eventId }
        storyboardJson.decodeFromString(StoryboardEvent.serializer(), row[DeadLetter.payload])
    }

    override suspend fun recordAudit(kind: String, target: String, detail: String, outcome: String): Unit = db.dbQuery {
        ReplayAudit.insert {
            it[this.kind] = kind
            it[this.target] = target
            it[this.detail] = detail
            it[this.outcome] = outcome
            it[createdAt] = Instant.now()
        }
    }

    override suspend fun listAudit(target: String): List<AuditEntry> = db.dbQuery {
        ReplayAudit.selectAll()
            .where { ReplayAudit.target eq target }
            .orderBy(ReplayAudit.id to SortOrder.ASC)
            .map {
                AuditEntry(
                    kind = it[ReplayAudit.kind],
                    target = it[ReplayAudit.target],
                    detail = it[ReplayAudit.detail],
                    outcome = it[ReplayAudit.outcome],
                    createdAt = it[ReplayAudit.createdAt].toString(),
                )
            }
    }

    override suspend fun resetStoryboard(storyboardId: String): Unit = db.dbQuery {
        SegmentProjection.deleteWhere { SegmentProjection.storyboardId eq storyboardId }
        AnalysisProjection.deleteWhere { AnalysisProjection.storyboardId eq storyboardId }
        ProcessedVersion.deleteWhere { ProcessedVersion.storyboardId eq storyboardId }
        PendingEvents.deleteWhere { PendingEvents.storyboardId eq storyboardId }
    }

    override suspend fun resetAll(): Unit = db.dbQuery {
        SegmentProjection.deleteAll()
        AnalysisProjection.deleteAll()
        ProcessedVersion.deleteAll()
        ProcessedEvent.deleteAll()
        PendingEvents.deleteAll()
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
