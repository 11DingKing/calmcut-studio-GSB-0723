package com.calmcut.studio.e2e

import com.calmcut.studio.config.AppConfig
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.redpanda.RedpandaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared real-infrastructure harness for end-to-end tests: a PostgreSQL 16
 * container and a Redpanda container (Kafka-API compatible). Containers are
 * started once per JVM and reused across test classes. Tests are skipped
 * gracefully (via [dockerAvailable]) when no Docker daemon is reachable.
 */
object E2EInfra {

    val dockerAvailable: Boolean by lazy {
        runCatching { DockerClientFactory.instance().client() }.isSuccess
    }

    val postgres: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer(DockerImageName.parse("postgres:16"))
            .withDatabaseName("calmcut")
            .withUsername("calmcut")
            .withPassword("calmcut")
            .also { it.start() }
    }

    val redpanda: RedpandaContainer by lazy {
        RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.2.7"))
            .also { it.start() }
    }

    fun dbConfig() = AppConfig.DbConfig(
        jdbcUrl = postgres.jdbcUrl,
        username = postgres.username,
        password = postgres.password,
        maxPoolSize = 8,
    )

    fun kafkaConfig(suffix: String) = AppConfig.KafkaConfig(
        bootstrapServers = redpanda.bootstrapServers,
        eventsTopic = "storyboard.events.$suffix",
        resultsTopic = "storyboard.results.$suffix",
        dlqTopic = "storyboard.dlq.$suffix",
        consumerGroup = "worker-$suffix",
    )
}
