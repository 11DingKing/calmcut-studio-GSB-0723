package com.calmcut.infrastructure.messaging

import com.calmcut.domain.events.*
import com.calmcut.infrastructure.repository.DeadLetterRecord
import com.calmcut.infrastructure.repository.DeadLetterRepository
import com.calmcut.service.EventProcessor
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class DeadLetterReplayer(
    private val deadLetterRepository: DeadLetterRepository,
    private val eventProcessor: EventProcessor,
    private val producerFactory: KafkaProducerFactory,
    private val topic: String,
    private val consumerGroupId: String
) {
    private val running = AtomicBoolean(false)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var job: Job? = null

    fun start() {
        running.set(true)
        job = scope.launch {
            logger.info { "Dead letter replayer started" }
            while (running.get() && isActive) {
                try {
                    val candidates = deadLetterRepository.getRetryableCandidates(20)
                    for (record in candidates) {
                        retryDeadLetter(record)
                    }
                    delay(10000)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error(e) { "Error in dead letter replayer" }
                    delay(30000)
                }
            }
        }
    }

    private suspend fun retryDeadLetter(record: DeadLetterRecord) {
        try {
            val event = decodeEvent(record.eventType, record.payload)
            val result = eventProcessor.processEvent(event, consumerGroupId)

            if (result.success) {
                deadLetterRepository.markRetried(record.id, success = true)
                logger.info { "Successfully replayed dead letter ${record.id} (event=${record.eventType})" }
            } else {
                deadLetterRepository.markRetried(
                    record.id,
                    success = false,
                    error = result.error ?: "Processing failed again"
                )
                logger.warn { "Dead letter ${record.id} replay failed: ${result.error}" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error retrying dead letter ${record.id}" }
            deadLetterRepository.markRetried(record.id, success = false, error = e.message)
        }
    }

    suspend fun replayToKafka(record: DeadLetterRecord): Boolean {
        return try {
            producerFactory.create().use { producer ->
                val kafkaRecord = ProducerRecord<String, String>(topic, record.storyboardId, record.payload)
                kafkaRecord.headers().add(RecordHeader("eventType", record.eventType.toByteArray()))
                kafkaRecord.headers().add(RecordHeader("replay", "true".toByteArray()))
                producer.send(kafkaRecord).get()
            }
            deadLetterRepository.markRetried(record.id, success = true)
            logger.info { "Replayed dead letter ${record.id} to Kafka topic $topic" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to replay dead letter ${record.id} to Kafka" }
            deadLetterRepository.markRetried(record.id, success = false, error = e.message)
            false
        }
    }

    fun stop() {
        running.set(false)
        runBlocking { job?.cancelAndJoin() }
        logger.info { "Dead letter replayer stopped" }
    }

    private fun decodeEvent(eventType: String, payload: String): DomainEvent {
        return when (eventType) {
            "STORYBOARD_CREATED" -> json.decodeFromString<StoryboardCreated>(payload)
            "SEGMENTS_BATCH_IMPORTED" -> json.decodeFromString<SegmentsBatchImported>(payload)
            "SEGMENT_ADDED" -> json.decodeFromString<SegmentAdded>(payload)
            "SEGMENT_UPDATED" -> json.decodeFromString<SegmentUpdated>(payload)
            "SEGMENT_DELETED" -> json.decodeFromString<SegmentDeleted>(payload)
            "SEGMENTS_REORDERED" -> json.decodeFromString<SegmentsReordered>(payload)
            "IMPORT_JOB_CREATED" -> json.decodeFromString<ImportJobCreated>(payload)
            "IMPORT_JOB_COMPLETED" -> json.decodeFromString<ImportJobCompleted>(payload)
            "IMPORT_JOB_CANCELLED" -> json.decodeFromString<ImportJobCancelled>(payload)
            "PROJECTION_REBUILD_REQUESTED" -> json.decodeFromString<ProjectionRebuildRequested>(payload)
            else -> throw IllegalArgumentException("Unknown event type for replay: $eventType")
        }
    }
}
