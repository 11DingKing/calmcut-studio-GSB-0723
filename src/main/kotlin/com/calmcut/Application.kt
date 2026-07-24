package com.calmcut

import com.calmcut.api.storyboardRoutes
import com.calmcut.domain.rules.RiskRuleConfig
import com.calmcut.infrastructure.db.DatabaseFactory
import com.calmcut.infrastructure.messaging.*
import com.calmcut.infrastructure.repository.*
import com.calmcut.service.*
import io.ktor.server.application.*
import io.ktor.server.config.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import java.util.concurrent.Executors

private val logger = KotlinLogging.logger {}

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        module()
    }.start(wait = true)
}

fun Application.module() {
    val config = environment.config
    val appComponents = Dependencies.configure(config)

    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
            isLenient = true
            ignoreUnknownKeys = true
            encodeDefaults = true
        })
    }

    install(StatusPages) {
        exception<Throwable> { call, cause ->
            logger.error(cause) { "Unhandled exception" }
            call.respond(
                HttpStatusCode.InternalServerError,
                mapOf("error" to (cause.message ?: "Internal server error"))
            )
        }
        exception<IllegalArgumentException> { call, cause ->
            call.respond(HttpStatusCode.BadRequest, mapOf("error" to (cause.message ?: "Bad request")))
        }
    }

    routing {
        storyboardRoutes(
            commandService = appComponents.commandService,
            queryService = appComponents.queryService,
            importService = appComponents.importService,
            eventProcessor = appComponents.eventProcessor
        )
    }

    appComponents.startBackgroundWorkers()

    environment.monitor.subscribe(ApplicationStopped) {
        appComponents.shutdown()
    }
}

class Dependencies private constructor(
    val dbFactory: DatabaseFactory,
    val storyboardRepository: StoryboardRepository,
    val eventLogRepository: EventLogRepository,
    val atomicWriteRepository: AtomicWriteRepository,
    val projectionRepository: RiskProjectionRepository,
    val deadLetterRepository: DeadLetterRepository,
    val importRepository: ImportRepository,
    val commandService: StoryboardCommandService,
    val queryService: StoryboardQueryService,
    val analysisEngine: RiskAnalysisEngine,
    val knowledgePointValidator: KnowledgePointValidator,
    val eventProcessor: EventProcessor,
    val importService: StreamingImportService,
    private val outboxPublisher: OutboxPublisher,
    private val eventConsumer: EventConsumer,
    private val deadLetterReplayer: DeadLetterReplayer,
    private val driftChecker: ProjectionDriftChecker,
    private val driftCheckIntervalSeconds: Long,
    private val workerScope: CoroutineScope
) {
    fun startBackgroundWorkers() {
        outboxPublisher.start()
        eventConsumer.start()
        deadLetterReplayer.start()
        driftChecker.start(workerScope, driftCheckIntervalSeconds)

        workerScope.launch {
            while (isActive) {
                try {
                    val published = outboxPublisher.publishPending()
                    if (published > 0) {
                        logger.debug { "Published $published outbox events" }
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Error in outbox polling loop" }
                }
                delay(500)
            }
        }
    }

    fun shutdown() {
        outboxPublisher.stop()
        eventConsumer.stop()
        deadLetterReplayer.stop()
        workerScope.cancel()
        dbFactory.close()
        logger.info { "Application shut down gracefully" }
    }

    companion object {
        fun configure(config: ApplicationConfig): Dependencies {
            val dbFactory = DatabaseFactory(config)
            dbFactory.connect()

            val storyboardRepo = StoryboardRepository()
            val eventLogRepo = EventLogRepository()
            val atomicWriteRepo = AtomicWriteRepository()
            val projectionRepo = RiskProjectionRepository()
            val deadLetterRepo = DeadLetterRepository()
            val importRepo = ImportRepository()

            val analysisConfig = RiskRuleConfig(
                ruleVersion = config.property("analysis.ruleVersion").getString(),
                windowSizeSeconds = config.property("analysis.windowSizeSeconds").getString().toInt(),
                maxReversalsPerWindow = config.property("analysis.maxReversalsPerWindow").getString().toInt(),
                consecutiveHighStimulus = config.property("analysis.consecutiveHighStimulus").getString().toInt(),
                highStimulusThreshold = config.property("analysis.highStimulusThreshold").getString().toInt(),
                minAverageShotSeconds = config.property("analysis.minAverageShotSeconds").getString().toDouble(),
                bufferStimulusThreshold = config.property("analysis.bufferStimulusThreshold").getString().toInt()
            )

            val analysisEngine = RiskAnalysisEngine(analysisConfig, storyboardRepo, projectionRepo)
            val kpValidator = KnowledgePointValidator(storyboardRepo)
            val eventProcessor = EventProcessor(storyboardRepo, atomicWriteRepo, projectionRepo, analysisEngine, kpValidator, analysisConfig)

            val redpandaConfig = config.config("redpanda")
            val bootstrapServers = redpandaConfig.property("bootstrapServers").getString()
            val consumerGroupId = redpandaConfig.property("consumerGroupId").getString()
            val topicEvents = redpandaConfig.property("topicEvents").getString()

            val producerFactory = KafkaProducerFactory(bootstrapServers)
            val consumerFactory = KafkaConsumerFactory(bootstrapServers, consumerGroupId)
            val outboxPublisher = OutboxPublisher(producerFactory, eventLogRepo)

            val importBatchSize = config.property("import.batchSize").getString().toInt()
            val maxConcurrent = config.property("import.maxConcurrentJobs").getString().toInt()
            val importService = StreamingImportService(storyboardRepo, atomicWriteRepo, importRepo, eventProcessor, importBatchSize, maxConcurrent, topicEvents)

            val eventConsumer = EventConsumer(
                consumerFactory = consumerFactory,
                atomicWriteRepository = atomicWriteRepo,
                deadLetterRepository = deadLetterRepo,
                topics = listOf(topicEvents),
                consumerGroupId = consumerGroupId,
                handler = DomainEventHandler { event -> eventProcessor.processEvent(event) }
            )

            val deadLetterReplayer = DeadLetterReplayer(
                deadLetterRepository = deadLetterRepo,
                eventProcessor = eventProcessor,
                producerFactory = producerFactory,
                topic = topicEvents,
                consumerGroupId = consumerGroupId
            )

            val driftCheckInterval = config.property("analysis.driftCheckIntervalSeconds").getString().toLong()
            val workerPoolSize = config.property("analysis.workerPoolSize").getString().toInt()
            val workerScope = CoroutineScope(Executors.newFixedThreadPool(workerPoolSize).asCoroutineDispatcher() + SupervisorJob())

            val driftChecker = ProjectionDriftChecker(projectionRepo, storyboardRepo, analysisEngine, eventProcessor, atomicWriteRepo)

            val commandService = StoryboardCommandService(storyboardRepo, atomicWriteRepo, eventLogRepo, kpValidator, topicEvents)
            val queryService = StoryboardQueryService(storyboardRepo, projectionRepo, analysisEngine, kpValidator)

            return Dependencies(
                dbFactory = dbFactory,
                storyboardRepository = storyboardRepo,
                eventLogRepository = eventLogRepo,
                atomicWriteRepository = atomicWriteRepo,
                projectionRepository = projectionRepo,
                deadLetterRepository = deadLetterRepo,
                importRepository = importRepo,
                commandService = commandService,
                queryService = queryService,
                analysisEngine = analysisEngine,
                knowledgePointValidator = kpValidator,
                eventProcessor = eventProcessor,
                importService = importService,
                outboxPublisher = outboxPublisher,
                eventConsumer = eventConsumer,
                deadLetterReplayer = deadLetterReplayer,
                driftChecker = driftChecker,
                driftCheckIntervalSeconds = driftCheckInterval,
                workerScope = workerScope
            )
        }
    }
}
