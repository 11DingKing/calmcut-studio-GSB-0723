package com.calmcut.studio

import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.api.AnalysisRoutes
import com.calmcut.studio.api.ImportRoutes
import com.calmcut.studio.api.StoryboardRoutes
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.event.EventStore
import com.calmcut.studio.event.KafkaEventPublisher
import com.calmcut.studio.event.OutboxRelay
import com.calmcut.studio.import.StreamingImporter
import com.calmcut.studio.projection.DriftDetector
import com.calmcut.studio.projection.KnowledgeIntegrityChecker
import com.calmcut.studio.projection.ProjectionRebuilder
import com.calmcut.studio.projection.ProjectionRepository
import com.calmcut.studio.projection.ProjectionUpdater
import com.calmcut.studio.worker.AnalysisWorker
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.json.Json
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        module()
    }.start(wait = true)
}

fun Application.module() {
    val appConfig = AppConfig.from(environment.config)

    logger.info { "Starting CalmCut Studio Risk Analysis Service" }
    logger.info { "Kafka bootstrap: ${appConfig.kafka.bootstrapServers}" }
    logger.info { "Rule version: ${appConfig.analysis.ruleVersion}" }

    DatabaseFactory.init(appConfig.db)

    val eventStore = EventStore()
    val publisher = KafkaEventPublisher(appConfig.kafka)
    val repository = ProjectionRepository()
    val analyzer = RiskAnalyzer(appConfig.analysis)
    val projectionUpdater = ProjectionUpdater(repository, analyzer, appConfig.analysis)
    val knowledgeIntegrityChecker = KnowledgeIntegrityChecker()
    val driftDetector = DriftDetector(repository, analyzer)
    val rebuilder = ProjectionRebuilder(eventStore, repository, projectionUpdater, analyzer)
    val importer = StreamingImporter(eventStore, appConfig.kafka.eventsTopic, appConfig.import)

    val outboxRelay = OutboxRelay(eventStore, publisher, appConfig.outbox)
    val worker = AnalysisWorker(
        config = appConfig,
        projectionUpdater = projectionUpdater,
        projectionRepository = repository,
        publisher = publisher,
        knowledgeIntegrityChecker = knowledgeIntegrityChecker
    )

    configureSerialization()
    configureHTTP()
    configureStatusPages()

    routing {
        StoryboardRoutes(eventStore, repository, appConfig).register(this)
        ImportRoutes(importer).register(this)
        AnalysisRoutes(repository, driftDetector, rebuilder).register(this)

        get("/health") {
            call.respond(mapOf("status" to "ok", "service" to "calmcut-studio"))
        }
    }

    outboxRelay.start()

    if (appConfig.analysis.incrementalMode) {
        worker.start()
        logger.info { "Risk analysis worker started" }
    }

    environment.monitor.subscribe(ApplicationStopping) {
        logger.info { "Shutting down..." }
        worker.stop()
        outboxRelay.stop()
        importer.shutdown()
        publisher.close()
        DatabaseFactory.close()
    }
}

private fun Application.configureSerialization() {
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = false
            isLenient = true
            ignoreUnknownKeys = true
            encodeDefaults = true
        })
    }
}

private fun Application.configureHTTP() {
    install(CORS) {
        anyHost()
        allowHeaders { true }
        allowMethod(io.ktor.http.HttpMethod.Options)
        allowMethod(io.ktor.http.HttpMethod.Get)
        allowMethod(io.ktor.http.HttpMethod.Post)
        allowMethod(io.ktor.http.HttpMethod.Put)
        allowMethod(io.ktor.http.HttpMethod.Delete)
    }
}

private fun Application.configureStatusPages() {
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(
                io.ktor.http.HttpStatusCode.BadRequest,
                mapOf("error" to "bad_request", "message" to (cause.message ?: "Invalid input"))
            )
        }
        exception<Throwable> { call, cause ->
            logger.error(cause) { "Unhandled exception" }
            call.respond(
                io.ktor.http.HttpStatusCode.InternalServerError,
                mapOf("error" to "internal_error", "message" to "An unexpected error occurred")
            )
        }
    }
}
