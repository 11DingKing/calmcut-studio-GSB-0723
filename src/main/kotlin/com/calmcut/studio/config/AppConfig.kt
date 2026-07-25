package com.calmcut.studio.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/** Strongly-typed view over application.conf's `storyboard` section. */
data class AppConfig(
    val db: DbConfig,
    val kafka: KafkaConfig,
    val outbox: OutboxConfig,
    val analysis: AnalysisConfig,
    val import: ImportConfig,
) {
    data class DbConfig(
        val jdbcUrl: String,
        val username: String,
        val password: String,
        val maxPoolSize: Int,
    )

    data class KafkaConfig(
        val bootstrapServers: String,
        val eventsTopic: String,
        val resultsTopic: String,
        val dlqTopic: String,
        val consumerGroup: String,
    )

    data class OutboxConfig(val pollIntervalMs: Long, val batchSize: Int)

    data class AnalysisConfig(
        val reversalWindowSeconds: Int,
        val maxReversalsPerWindow: Int,
        val consecutiveIntensityThreshold: Int,
        val consecutiveIntensityCount: Int,
        val minAverageShotSeconds: Double,
    )

    data class ImportConfig(val chunkSize: Int)

    companion object {
        fun load(config: Config = ConfigFactory.load()): AppConfig {
            val s = config.getConfig("storyboard")
            val db = s.getConfig("database")
            val k = s.getConfig("kafka")
            val o = s.getConfig("outbox")
            val a = s.getConfig("analysis")
            val i = s.getConfig("import")
            return AppConfig(
                db = DbConfig(
                    jdbcUrl = db.getString("jdbcUrl"),
                    username = db.getString("username"),
                    password = db.getString("password"),
                    maxPoolSize = db.getInt("maxPoolSize"),
                ),
                kafka = KafkaConfig(
                    bootstrapServers = k.getString("bootstrapServers"),
                    eventsTopic = k.getString("eventsTopic"),
                    resultsTopic = k.getString("resultsTopic"),
                    dlqTopic = k.getString("dlqTopic"),
                    consumerGroup = k.getString("consumerGroup"),
                ),
                outbox = OutboxConfig(o.getLong("pollIntervalMs"), o.getInt("batchSize")),
                analysis = AnalysisConfig(
                    reversalWindowSeconds = a.getInt("reversalWindowSeconds"),
                    maxReversalsPerWindow = a.getInt("maxReversalsPerWindow"),
                    consecutiveIntensityThreshold = a.getInt("consecutiveIntensityThreshold"),
                    consecutiveIntensityCount = a.getInt("consecutiveIntensityCount"),
                    minAverageShotSeconds = a.getDouble("minAverageShotSeconds"),
                ),
                import = ImportConfig(i.getInt("chunkSize")),
            )
        }
    }
}
