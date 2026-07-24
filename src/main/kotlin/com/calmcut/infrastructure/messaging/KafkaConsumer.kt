package com.calmcut.infrastructure.messaging

import com.calmcut.domain.events.*
import com.calmcut.infrastructure.repository.ConsumerOffsetRepository
import com.calmcut.infrastructure.repository.DeadLetterRepository
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

class KafkaConsumerFactory(
    private val bootstrapServers: String,
    private val consumerGroupId: String
) {
    fun create(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)
            put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000)
        }
        return KafkaConsumer(props)
    }
}

data class BufferedEvent(
    val event: DomainEvent,
    val record: ConsumerRecord<String, String>
)

class OutOfOrderEventBuffer {
    private val buffers = ConcurrentHashMap<String, TreeMap<Long, BufferedEvent>>()
    private val processedVersions = ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<Long, Boolean>>()
    private val mutex = Mutex()

    suspend fun addAndGetReady(storyboardId: String, expectedNextVersion: Long, event: DomainEvent, record: ConsumerRecord<String, String>): List<BufferedEvent> {
        mutex.withLock {
            val version = event.aggregateVersion
            val processed = processedVersions.computeIfAbsent(storyboardId) { ConcurrentHashMap.newKeySet() }

            if (processed.contains(version)) {
                logger.debug { "Duplicate event version $version for storyboard $storyboardId, skipping" }
                return emptyList()
            }

            val buffer = buffers.computeIfAbsent(storyboardId) { TreeMap() }
            buffer[version] = BufferedEvent(event, record)

            val ready = mutableListOf<BufferedEvent>()
            var nextVersion = expectedNextVersion
            while (buffer.containsKey(nextVersion)) {
                val buffered = buffer.remove(nextVersion)!!
                ready.add(buffered)
                processed.add(nextVersion)
                nextVersion++
            }

            if (buffer.size > 1000) {
                logger.warn { "Buffer for storyboard $storyboardId has ${buffer.size} events, possible missing version" }
            }

            return ready
        }
    }

    suspend fun markProcessed(storyboardId: String, version: Long) {
        mutex.withLock {
            processedVersions.computeIfAbsent(storyboardId) { ConcurrentHashMap.newKeySet() }.add(version)
        }
    }

    suspend fun getBufferedCount(storyboardId: String): Int {
        mutex.withLock {
            return buffers[storyboardId]?.size ?: 0
        }
    }

    suspend fun clear(storyboardId: String) {
        mutex.withLock {
            buffers.remove(storyboardId)
            processedVersions.remove(storyboardId)
        }
    }
}

data class MessageHandlerResult(
    val success: Boolean,
    val processedVersion: Long? = null,
    val error: String? = null
)

fun interface DomainEventHandler {
    suspend fun handle(event: DomainEvent): MessageHandlerResult
}

class EventConsumer(
    private val consumerFactory: KafkaConsumerFactory,
    private val offsetRepository: ConsumerOffsetRepository,
    private val deadLetterRepository: DeadLetterRepository,
    private val topics: List<String>,
    private val handler: DomainEventHandler
) {
    private val running = AtomicBoolean(false)
    private val buffer = OutOfOrderEventBuffer()
    private val storyboardVersions = ConcurrentHashMap<String, Long>()
    private val pendingCommits = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()
    private var job: Job? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    fun start() {
        running.set(true)
        job = scope.launch {
            consumerFactory.create().use { consumer ->
                consumer.subscribe(topics)
                logger.info { "Event consumer started for topics: $topics" }

                while (running.get() && isActive) {
                    try {
                        val records = consumer.poll(Duration.ofMillis(1000))
                        if (records.isEmpty) {
                            yield()
                            continue
                        }

                        for (record in records) {
                            processRecord(record)
                        }

                        flushCommits(consumer)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logger.error(e) { "Error in consumer poll loop" }
                        delay(1000)
                    }
                }
            }
        }
    }

    private suspend fun processRecord(record: ConsumerRecord<String, String>) {
        val tp = TopicPartition(record.topic(), record.partition())
        val storyboardId = record.key()

        try {
            val event = decodeEvent(record.value())
            val currentVersion = storyboardVersions.getOrDefault(storyboardId, 0L)
            val expectedNext = currentVersion + 1

            if (event.aggregateVersion <= currentVersion) {
                logger.debug { "Event ${event.eventType} version ${event.aggregateVersion} already processed for $storyboardId (current=$currentVersion)" }
                pendingCommits[tp] = OffsetAndMetadata(record.offset() + 1)
                return
            }

            if (event.aggregateVersion > expectedNext) {
                logger.debug { "Out-of-order event: expected $expectedNext got ${event.aggregateVersion} for $storyboardId, buffering" }
                val ready = buffer.addAndGetReady(storyboardId, expectedNext, event, record)
                for (buffered in ready) {
                    val result = handler.handle(buffered.event)
                    if (result.success && result.processedVersion != null) {
                        storyboardVersions[storyboardId] = result.processedVersion
                        buffer.markProcessed(storyboardId, result.processedVersion)
                    }
                }
                pendingCommits[tp] = OffsetAndMetadata(record.offset() + 1)
                return
            }

            val result = handler.handle(event)
            if (result.success && result.processedVersion != null) {
                storyboardVersions[storyboardId] = result.processedVersion
                buffer.markProcessed(storyboardId, result.processedVersion)

                val extraReady = buffer.addAndGetReady(storyboardId, result.processedVersion + 1, event, record)
                for (buffered in extraReady) {
                    val extraResult = handler.handle(buffered.event)
                    if (extraResult.success && extraResult.processedVersion != null) {
                        storyboardVersions[storyboardId] = extraResult.processedVersion
                        buffer.markProcessed(storyboardId, extraResult.processedVersion)
                    }
                }
            } else {
                deadLetterRepository.save(
                    originalTopic = record.topic(),
                    originalPartition = record.partition(),
                    originalOffset = record.offset(),
                    eventType = event.eventType,
                    storyboardId = storyboardId,
                    payload = record.value(),
                    errorMessage = result.error ?: "Unknown handling error",
                    errorStackTrace = null
                )
            }

            pendingCommits[tp] = OffsetAndMetadata(record.offset() + 1)
        } catch (e: Exception) {
            logger.error(e) { "Failed to process record at ${record.topic()}-${record.partition()}:${record.offset()}" }
            try {
                deadLetterRepository.save(
                    originalTopic = record.topic(),
                    originalPartition = record.partition(),
                    originalOffset = record.offset(),
                    eventType = "UNKNOWN",
                    storyboardId = storyboardId,
                    payload = record.value(),
                    errorMessage = e.message ?: "Deserialization/processing error",
                    errorStackTrace = e.stackTraceToString()
                )
            } catch (dlqError: Exception) {
                logger.error(dlqError) { "Failed to write to dead letter queue" }
            }
            pendingCommits[tp] = OffsetAndMetadata(record.offset() + 1)
        }
    }

    private fun flushCommits(consumer: KafkaConsumer<String, String>) {
        if (pendingCommits.isNotEmpty()) {
            try {
                consumer.commitSync(HashMap(pendingCommits))
                pendingCommits.clear()
            } catch (e: Exception) {
                logger.error(e) { "Failed to commit offsets" }
            }
        }
    }

    fun stop() {
        running.set(false)
        runBlocking { job?.cancelAndJoin() }
        logger.info { "Event consumer stopped" }
    }

    suspend fun rebuildStoryboardVersion(storyboardId: String, version: Long) {
        storyboardVersions[storyboardId] = version
        buffer.clear(storyboardId)
    }

    private fun decodeEvent(payload: String): DomainEvent {
        val node = json.parseToJsonElement(payload)
        val eventType = node.jsonObject["eventType"]?.jsonPrimitive?.content
            ?: throw IllegalArgumentException("Missing eventType in payload")
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
            else -> throw IllegalArgumentException("Unknown event type: $eventType")
        }
    }
}
