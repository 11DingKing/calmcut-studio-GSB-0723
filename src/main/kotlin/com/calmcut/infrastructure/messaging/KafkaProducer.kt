package com.calmcut.infrastructure.messaging

import com.calmcut.infrastructure.repository.EventLogRepository
import com.calmcut.infrastructure.repository.OutboxEventRecord
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class KafkaProducerFactory(bootstrapServers: String) {
    private val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.RETRIES_CONFIG, 3)
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
        put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000)
        put(ProducerConfig.LINGER_MS_CONFIG, 5)
        put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
        put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    }

    fun create(): KafkaProducer<String, String> = KafkaProducer(producerProps)
}

class OutboxPublisher(
    private val producerFactory: KafkaProducerFactory,
    private val eventLogRepository: EventLogRepository
) {
    private val running = AtomicBoolean(false)
    private var producer: KafkaProducer<String, String>? = null

    fun start() {
        running.set(true)
        producer = producerFactory.create()
        logger.info { "Outbox publisher started" }
    }

    fun stop() {
        running.set(false)
        producer?.close()
        producer = null
        logger.info { "Outbox publisher stopped" }
    }

    suspend fun publishPending(): Int {
        var published = 0
        val prod = producer ?: return 0

        while (running.get()) {
            val events = eventLogRepository.getUnpublishedOutboxEvents(100)
            if (events.isEmpty()) break

            for (event in events) {
                try {
                    val record = ProducerRecord<String, String>(
                        event.topic,
                        event.key,
                        event.payload
                    ).apply {
                        event.headers.forEach { (k, v) ->
                            headers().add(RecordHeader(k, v.toByteArray()))
                        }
                        headers().add(RecordHeader("eventId", event.eventId.toByteArray()))
                        headers().add(RecordHeader("eventType", event.eventType.toByteArray()))
                    }

                    withContext(Dispatchers.IO) { prod.send(record).get() }
                    eventLogRepository.markOutboxPublished(event.id)
                    published++
                    logger.debug { "Published event ${event.eventId} to ${event.topic}" }
                } catch (e: Exception) {
                    logger.error(e) { "Failed to publish event ${event.eventId}" }
                    eventLogRepository.markOutboxFailed(event.id, e.message ?: "Unknown error")
                }
            }
        }
        return published
    }

    suspend fun publishSingle(event: OutboxEventRecord): Boolean {
        val prod = producer ?: return false
        return try {
            val record = ProducerRecord<String, String>(
                event.topic, event.key, event.payload
            ).apply {
                event.headers.forEach { (k, v) ->
                    headers().add(RecordHeader(k, v.toByteArray()))
                }
            }
            withContext(Dispatchers.IO) { prod.send(record).get() }
            eventLogRepository.markOutboxPublished(event.id)
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to publish single event ${event.eventId}" }
            eventLogRepository.markOutboxFailed(event.id, e.message ?: "Unknown error")
            false
        }
    }
}
