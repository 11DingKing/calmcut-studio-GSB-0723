package com.calmcut.studio.messaging

import com.calmcut.studio.config.AppConfig
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Thin wrapper over a Kafka (Redpanda-compatible) producer with idempotent,
 * acks=all delivery so the outbox publisher gets at-least-once semantics with
 * per-key ordering (key = storyboardId).
 */
class KafkaEventProducer(kafka: AppConfig.KafkaConfig) : AutoCloseable {

    private val producer = KafkaProducer<String, String>(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
        }
    )

    /** Publishes synchronously (blocks until acked) so callers can commit safely. */
    fun send(topic: String, key: String, value: String) {
        producer.send(ProducerRecord(topic, key, value)).get()
    }

    override fun close() = producer.close()
}
