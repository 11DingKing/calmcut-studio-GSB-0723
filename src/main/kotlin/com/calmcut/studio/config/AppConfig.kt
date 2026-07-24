package com.calmcut.studio.config

import io.ktor.server.config.*

data class AppConfig(
    val db: DbConfig,
    val kafka: KafkaConfig,
    val analysis: AnalysisConfig,
    val import: ImportConfig,
    val outbox: OutboxConfig
) {
    data class DbConfig(
        val jdbcUrl: String,
        val username: String,
        val password: String,
        val maximumPoolSize: Int
    )

    data class KafkaConfig(
        val bootstrapServers: String,
        val consumerGroup: String,
        val eventsTopic: String,
        val resultsTopic: String,
        val dlqTopic: String,
        val producerAcks: String,
        val consumerAutoOffsetReset: String
    )

    data class AnalysisConfig(
        val ruleVersion: String,
        val windowSizeSeconds: Int,
        val maxReversalsPerWindow: Int,
        val consecutiveIntensityCount: Int,
        val maxIntensity: Int,
        val minAvgShotSeconds: Double,
        val bufferIntensityThreshold: Int,
        val incrementalMode: Boolean,
        val driftCheckEnabled: Boolean,
        val driftCheckSampleRate: Double
    ) {
        val windowSizeMs: Long get() = windowSizeSeconds * 1000L
        val minAvgShotMs: Long get() = (minAvgShotSeconds * 1000).toLong()
    }

    data class ImportConfig(
        val chunkSize: Int,
        val maxConcurrentChunks: Int
    )

    data class OutboxConfig(
        val pollIntervalMs: Long,
        val batchSize: Int
    )

    companion object {
        fun from(config: ApplicationConfig): AppConfig {
            val c = config.config("calmcut")
            return AppConfig(
                db = DbConfig(
                    jdbcUrl = c.property("db.jdbcUrl").getString(),
                    username = c.property("db.username").getString(),
                    password = c.property("db.password").getString(),
                    maximumPoolSize = c.property("db.maximumPoolSize").getString().toInt()
                ),
                kafka = KafkaConfig(
                    bootstrapServers = c.property("kafka.bootstrapServers").getString(),
                    consumerGroup = c.property("kafka.consumerGroup").getString(),
                    eventsTopic = c.property("kafka.eventsTopic").getString(),
                    resultsTopic = c.property("kafka.resultsTopic").getString(),
                    dlqTopic = c.property("kafka.dlqTopic").getString(),
                    producerAcks = c.property("kafka.producerAcks").getString(),
                    consumerAutoOffsetReset = c.property("kafka.consumerAutoOffsetReset").getString()
                ),
                analysis = c.config("analysis").let { a ->
                    AnalysisConfig(
                        ruleVersion = a.property("ruleVersion").getString(),
                        windowSizeSeconds = a.property("windowSizeSeconds").getString().toInt(),
                        maxReversalsPerWindow = a.property("maxReversalsPerWindow").getString().toInt(),
                        consecutiveIntensityCount = a.property("consecutiveIntensityCount").getString().toInt(),
                        maxIntensity = a.property("maxIntensity").getString().toInt(),
                        minAvgShotSeconds = a.property("minAvgShotSeconds").getString().toDouble(),
                        bufferIntensityThreshold = a.property("bufferIntensityThreshold").getString().toInt(),
                        incrementalMode = a.property("incrementalMode").getString().toBoolean(),
                        driftCheckEnabled = a.property("driftCheckEnabled").getString().toBoolean(),
                        driftCheckSampleRate = a.property("driftCheckSampleRate").getString().toDouble()
                    )
                },
                import = c.config("import").let { i ->
                    ImportConfig(
                        chunkSize = i.property("chunkSize").getString().toInt(),
                        maxConcurrentChunks = i.property("maxConcurrentChunks").getString().toInt()
                    )
                },
                outbox = c.config("outbox").let { o ->
                    OutboxConfig(
                        pollIntervalMs = o.property("pollIntervalMs").getString().toLong(),
                        batchSize = o.property("batchSize").getString().toInt()
                    )
                }
            )
        }
    }
}
