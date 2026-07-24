package com.calmcut.studio.messaging

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.Outbox
import java.time.Instant
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory

/**
 * Drains the transactional outbox to Kafka ("写入 API 通过 outbox 保证事件最终发布").
 * Reads unpublished rows in id order, sends them (keyed by storyboardId to
 * preserve per-storyboard ordering), and marks them published. Because sends are
 * at-least-once and consumers are idempotent, a crash between send and mark
 * results only in a harmless duplicate delivery.
 */
class OutboxPublisher(
    private val db: DatabaseFactory,
    private val producer: KafkaEventProducer,
    private val config: AppConfig.OutboxConfig,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun start(scope: CoroutineScope): Job = scope.launch {
        log.info("outbox publisher started (interval={}ms batch={})", config.pollIntervalMs, config.batchSize)
        while (isActive) {
            try {
                val published = drainOnce()
                if (published == 0) delay(config.pollIntervalMs)
            } catch (t: Throwable) {
                log.error("outbox drain failed", t)
                delay(config.pollIntervalMs)
            }
        }
    }

    /** Publishes one batch; returns the number of rows published. */
    suspend fun drainOnce(): Int {
        val batch = db.dbQuery {
            Outbox.selectAll()
                .where { Outbox.published eq false }
                .orderBy(Outbox.id to SortOrder.ASC)
                .limit(config.batchSize)
                .map {
                    OutboxRow(
                        id = it[Outbox.id].value,
                        eventId = it[Outbox.eventId],
                        storyboardId = it[Outbox.storyboardId],
                        topic = it[Outbox.topic],
                        payload = it[Outbox.payload],
                    )
                }
        }
        for (row in batch) {
            producer.send(row.topic, row.storyboardId, row.payload)
            db.dbQuery {
                Outbox.update({ Outbox.id eq row.id }) {
                    it[published] = true
                    it[publishedAt] = Instant.now()
                }
            }
        }
        return batch.size
    }

    private data class OutboxRow(
        val id: Long,
        val eventId: String,
        val storyboardId: String,
        val topic: String,
        val payload: String,
    )
}
