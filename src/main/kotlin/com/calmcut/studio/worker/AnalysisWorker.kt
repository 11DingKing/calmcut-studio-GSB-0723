package com.calmcut.studio.worker

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.event.KafkaEventPublisher
import com.calmcut.studio.event.StoryboardEvent
import com.calmcut.studio.event.eventJson
import com.calmcut.studio.projection.KnowledgeIntegrityChecker
import com.calmcut.studio.projection.OptimisticLockException
import com.calmcut.studio.projection.ProjectionRepository
import com.calmcut.studio.projection.ProjectionResult
import com.calmcut.studio.projection.ProjectionUpdater
import kotlinx.coroutines.*
import kotlinx.serialization.encodeToString
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

class AnalysisWorker(
    private val config: AppConfig,
    private val projectionUpdater: ProjectionUpdater,
    private val projectionRepository: ProjectionRepository,
    private val publisher: KafkaEventPublisher,
    private val knowledgeIntegrityChecker: KnowledgeIntegrityChecker,
    private val idempotencyChecker: IdempotencyChecker = IdempotentProcessor(config.kafka.consumerGroup),
    private val deadLetterSink: DeadLetterSink = DeadLetterQueue(config.kafka.consumerGroup)
) {
    private val running = AtomicBoolean(false)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val versionBuffer = VersionBuffer()
    private var consumer: KafkaConsumer<String, String>? = null
    private var consumerJob: Job? = null

    val versionBufferRef get() = versionBuffer

    fun start() {
        if (!running.compareAndSet(false, true)) return
        logger.info { "Starting analysis worker for topic ${config.kafka.eventsTopic}" }

        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, config.kafka.consumerGroup)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.kafka.consumerAutoOffsetReset)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)
            put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000)
            put(ConsumerConfig.CLIENT_ID_CONFIG, "calmcut-worker-${UUID.randomUUID().toString().take(8)}")
            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
        }

        consumer = KafkaConsumer(props)
        consumerJob = scope.launch {
            try {
                consumer?.subscribe(
                    listOf(config.kafka.eventsTopic),
                    object : ConsumerRebalanceListener {
                        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                            logger.info { "Partitions revoked: $partitions" }
                        }
                        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                            logger.info { "Partitions assigned: $partitions" }
                        }
                    }
                )

                while (running.get() && isActive) {
                    val records = try {
                        consumer?.poll(Duration.ofMillis(500))
                    } catch (e: Exception) {
                        logger.error(e) { "Error polling Kafka" }
                        delay(1000)
                        continue
                    } ?: continue

                    if (records.isEmpty) continue

                    for (record in records) {
                        try {
                            processRecord(record.value())
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            logger.error(e) { "Error processing record at partition=${record.partition()} offset=${record.offset()}" }
                            runCatching {
                                runBlocking {
                                    deadLetterSink.enqueue(
                                        eventId = record.key() ?: UUID.randomUUID().toString(),
                                        topic = record.topic(),
                                        partition = record.partition(),
                                        offset = record.offset(),
                                        payload = record.value(),
                                        error = e
                                    )
                                }
                            }
                        }
                    }

                    try {
                        consumer?.commitSync()
                    } catch (e: CommitFailedException) {
                        logger.warn(e) { "Commit failed, will retry on next poll" }
                    }
                }
            } catch (e: CancellationException) {
                logger.info { "Worker loop cancelled" }
            } catch (e: Exception) {
                logger.error(e) { "Fatal error in worker loop" }
            }
        }
    }

    suspend fun processRecord(payload: String): ProjectionResult? {
        val event = try {
            eventJson.decodeFromString<StoryboardEvent>(payload)
        } catch (e: Exception) {
            logger.error(e) { "Failed to deserialize event" }
            return null
        }

        if (idempotencyChecker.isProcessed(event.eventId)) {
            logger.debug { "Duplicate event ${event.eventId} (${event.eventType}), skipping" }
            return null
        }

        val startVersion = versionBuffer.getProcessedVersion(event.aggregateId)
        val bufferResult = versionBuffer.offer(event)
        val eventsToProcess = when (bufferResult) {
            is VersionBuffer.BufferResult.Ready -> bufferResult.events
            is VersionBuffer.BufferResult.Buffered -> {
                logger.debug { "Event ${event.eventId} v${event.version} buffered (waiting for v${bufferResult.expectedVersion})" }
                return null
            }
            is VersionBuffer.BufferResult.Duplicate -> {
                logger.debug { "Stale event ${event.eventId} v${event.version} (already at v${versionBuffer.getProcessedVersion(event.aggregateId)})" }
                idempotencyChecker.markProcessed(event.eventId, "stale")
                return null
            }
        }

        var lastResult: ProjectionResult? = null
        for (evt in eventsToProcess) {
            try {
                val result = projectionUpdater.applyEvent(evt)
                lastResult = result

                val timeline = projectionRepository.loadTimeline(evt.timelineId)
                if (timeline != null) {
                    val integrityResults = knowledgeIntegrityChecker.verify(timeline.segments)
                    if (integrityResults.isNotEmpty()) {
                        knowledgeIntegrityChecker.saveResults(evt.timelineId, integrityResults)
                    }
                }

                publisher.publishResult(evt.timelineId, eventJson.encodeToString(result.analysisResult))

                idempotencyChecker.markProcessed(evt.eventId)
                logger.info { "Processed event ${evt.eventId} (${evt.eventType}) v${evt.version} for timeline ${evt.timelineId}, found ${result.analysisResult.findings.size} risks" }
            } catch (e: OptimisticLockException) {
                logger.warn(e) { "Optimistic lock conflict for ${evt.aggregateId}, rolling back buffer to v$startVersion" }
                versionBuffer.setProcessedVersion(evt.aggregateId, startVersion)
                idempotencyChecker.markFailed(evt.eventId)
                throw e
            } catch (e: Exception) {
                logger.error(e) { "Error applying event ${evt.eventId}, rolling back buffer to v$startVersion" }
                versionBuffer.setProcessedVersion(evt.aggregateId, startVersion)
                idempotencyChecker.markFailed(evt.eventId)
                throw e
            }
        }

        return lastResult
    }

    suspend fun replayDlq(limit: Int = 100): Int {
        val entries = deadLetterSink.getUnreplayed(limit)
        var replayed = 0
        for (entry in entries) {
            try {
                processRecord(entry.payload)
                deadLetterSink.markReplayed(entry.id)
                replayed++
            } catch (e: Exception) {
                logger.error(e) { "DLQ replay failed for entry ${entry.id}" }
                deadLetterSink.incrementAttempt(entry.id)
            }
        }
        return replayed
    }

    fun stop() {
        logger.info { "Stopping analysis worker" }
        running.set(false)
        consumerJob?.cancel()
        scope.cancel()
        consumer?.wakeup()
        consumer?.close(Duration.ofSeconds(5))
        consumer = null
    }
}
