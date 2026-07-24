package com.calmcut.studio.event

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import kotlinx.coroutines.*
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class OutboxRelay(
    private val eventStore: EventStore,
    private val publisher: KafkaEventPublisher,
    private val config: AppConfig.OutboxConfig
) {
    private var job: Job? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    fun start() {
        job = scope.launch {
            logger.info { "Outbox relay started, polling every ${config.pollIntervalMs}ms" }
            while (isActive) {
                try {
                    val batch = eventStore.getUnpublishedOutbox(config.batchSize)
                    if (batch.isEmpty()) {
                        delay(config.pollIntervalMs)
                        continue
                    }
                    logger.debug { "Relaying ${batch.size} outbox entries" }
                    for (entry in batch) {
                        try {
                            publisher.publish(
                                topic = entry.topic,
                                key = entry.key,
                                payload = entry.payload,
                                headers = mapOf("outbox_id" to entry.id.toString())
                            )
                            eventStore.markPublished(entry.id)
                        } catch (e: Exception) {
                            logger.error(e) { "Failed to relay outbox entry ${entry.id}" }
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error(e) { "Outbox relay poll error" }
                    delay(config.pollIntervalMs)
                }
            }
        }
    }

    fun stop() {
        logger.info { "Stopping outbox relay" }
        job?.cancel()
        job = null
    }
}
