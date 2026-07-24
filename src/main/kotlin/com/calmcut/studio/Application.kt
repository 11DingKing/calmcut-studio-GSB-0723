package com.calmcut.studio

import com.calmcut.studio.analysis.AnalysisSettings
import com.calmcut.studio.analysis.RiskAnalyzer
import com.calmcut.studio.api.ErrorResponse
import com.calmcut.studio.api.StoryboardWriteService
import com.calmcut.studio.api.storyboardRoutes
import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.db.DatabaseFactory
import com.calmcut.studio.db.DbWorkerRepository
import com.calmcut.studio.db.EventStore
import com.calmcut.studio.db.IntegrityViolationException
import com.calmcut.studio.db.KafkaResultPublisher
import com.calmcut.studio.db.OptimisticLockException
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.messaging.AnalysisConsumer
import com.calmcut.studio.messaging.KafkaEventProducer
import com.calmcut.studio.messaging.OutboxPublisher
import com.calmcut.studio.worker.IdempotentProcessor
import com.calmcut.studio.worker.RebuildService
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.netty.EngineMain
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.routing.routing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.serialization.json.Json

fun main(args: Array<String>) = EngineMain.main(args)

fun Application.module() {
    val appConfig = AppConfig.load()
    val analysisSettings = AnalysisSettings(
        reversalWindowMs = appConfig.analysis.reversalWindowSeconds * 1000L,
        maxReversalsPerWindow = appConfig.analysis.maxReversalsPerWindow,
        consecutiveIntensityValue = appConfig.analysis.consecutiveIntensityThreshold,
        consecutiveIntensityCount = appConfig.analysis.consecutiveIntensityCount,
        minAverageShotMs = (appConfig.analysis.minAverageShotSeconds * 1000).toLong(),
    )

    val db = DatabaseFactory(appConfig.db).apply { connect() }
    val eventStore = EventStore(appConfig.kafka.eventsTopic, db)
    val producer = KafkaEventProducer(appConfig.kafka)
    val analyzer = RiskAnalyzer(analysisSettings)

    val repo = DbWorkerRepository(db)
    val resultPublisher = KafkaResultPublisher(producer, appConfig.kafka)
    val processor = IdempotentProcessor(analyzer, repo, resultPublisher)
    val rebuilds = RebuildService(eventStore, repo, analyzer)

    val writes = StoryboardWriteService(db, eventStore)
    val imports = StreamingImportService(db, eventStore, appConfig.import.chunkSize)

    val appScope = CoroutineScope(SupervisorJob())
    OutboxPublisher(db, producer, appConfig.outbox).start(appScope)
    AnalysisConsumer(appConfig.kafka, processor).start(appScope)

    install(ContentNegotiation) { json(Json { encodeDefaults = true; ignoreUnknownKeys = true }) }
    configureStatusPages()

    routing {
        storyboardRoutes(writes, repo, imports, processor, rebuilds, appScope)
    }

    monitor.subscribe(io.ktor.server.application.ApplicationStopped) {
        appScope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        producer.close()
        db.close()
    }
}

fun Application.configureStatusPages() {
    install(StatusPages) {
        exception<OptimisticLockException> { call, cause ->
            call.respond(HttpStatusCode.Conflict, ErrorResponse(cause.message ?: "optimistic lock conflict"))
        }
        exception<IntegrityViolationException> { call, cause ->
            call.respond(HttpStatusCode.UnprocessableEntity, ErrorResponse("integrity violation", cause.violations))
        }
        exception<IllegalArgumentException> { call, cause ->
            call.respond(HttpStatusCode.BadRequest, ErrorResponse(cause.message ?: "invalid request"))
        }
        exception<Throwable> { call, cause ->
            call.respond(HttpStatusCode.InternalServerError, ErrorResponse(cause.message ?: "internal error"))
        }
    }
}
