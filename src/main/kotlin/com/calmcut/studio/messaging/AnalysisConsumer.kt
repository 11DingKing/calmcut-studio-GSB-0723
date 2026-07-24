package com.calmcut.studio.messaging

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.storyboardJson
import com.calmcut.studio.domain.StoryboardEvent
import com.calmcut.studio.worker.IdempotentProcessor
import java.time.Duration
import java.util.Properties
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

/**
 * Consumes storyboard events from Kafka/Redpanda and feeds them to the
 * [IdempotentProcessor]. Offsets are committed only after a poll batch is fully
 * processed; combined with idempotent processing this yields safe at-least-once
 * semantics across worker crashes.
 */
class AnalysisConsumer(
    private val kafka: AppConfig.KafkaConfig,
    private val processor: IdempotentProcessor,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun start(scope: CoroutineScope): Job = scope.launch(Dispatchers.IO) {
        val consumer = KafkaConsumer<String, String>(
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, kafka.consumerGroup)
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
            }
        )
        consumer.subscribe(listOf(kafka.eventsTopic))
        log.info("analysis consumer subscribed to {}", kafka.eventsTopic)
        try {
            while (isActive) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (record in records) {
                    val event = runCatching {
                        storyboardJson.decodeFromString(StoryboardEvent.serializer(), record.value())
                    }.getOrNull() ?: continue
                    processor.process(event)
                }
                if (!records.isEmpty) consumer.commitSync()
            }
        } catch (t: Throwable) {
            log.error("consumer loop terminated", t)
        } finally {
            consumer.close()
        }
    }
}
