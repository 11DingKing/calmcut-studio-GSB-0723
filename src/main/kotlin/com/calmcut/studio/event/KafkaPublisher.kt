package com.calmcut.studio.event

import com.calmcut.studio.config.AppConfig
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID

private val logger = KotlinLogging.logger {}

class KafkaEventPublisher(private val config: AppConfig.KafkaConfig) {
    private val producer: KafkaProducer<String, String> by lazy {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, config.producerAcks)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            put(ProducerConfig.CLIENT_ID_CONFIG, "calmcut-publisher-${UUID.randomUUID().toString().take(8)}")
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
            put(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE)
            put(ProducerConfig.LINGER_MS_CONFIG, 5)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 32768)
        }
        KafkaProducer(props)
    }

    fun publish(topic: String, key: String, payload: String, headers: Map<String, String> = emptyMap()) {
        val recordHeaders = RecordHeaders()
        headers.forEach { (k, v) -> recordHeaders.add(k, v.toByteArray()) }
        recordHeaders.add("event_id", key.toByteArray())
        recordHeaders.add("published_at", System.currentTimeMillis().toString().toByteArray())

        val record = ProducerRecord(topic, null, key, payload, recordHeaders)
        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error(exception) { "Failed to publish event to $topic with key $key" }
            } else {
                logger.debug { "Published event to $topic partition=${metadata.partition()} offset=${metadata.offset()}" }
            }
        }
    }

    fun publishResult(timelineId: String, resultJson: String) {
        publish(config.resultsTopic, timelineId, resultJson)
    }

    fun close() {
        logger.info { "Closing Kafka producer" }
        producer.flush()
        producer.close()
    }
}
