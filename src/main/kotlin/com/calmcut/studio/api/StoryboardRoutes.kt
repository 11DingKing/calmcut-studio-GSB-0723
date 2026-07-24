package com.calmcut.studio.api

import com.calmcut.studio.config.AppConfig
import com.calmcut.studio.domain.model.StoryboardSegment
import com.calmcut.studio.event.*
import com.calmcut.studio.import.StreamingImporter
import com.calmcut.studio.projection.OptimisticLockException
import com.calmcut.studio.projection.ProjectionRepository
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

class StoryboardRoutes(
    private val eventStore: EventStore,
    private val projectionRepository: ProjectionRepository,
    private val config: AppConfig
) {
    fun register(routing: Routing) {
        with(routing) {
            route("/api/timelines/{timelineId}") {
                get {
                    val timelineId = call.parameters["timelineId"]!!
                    val state = projectionRepository.loadTimeline(timelineId)
                    if (state == null) {
                        call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Timeline not found"))
                        return@get
                    }
                    call.respond(
                        TimelineResponse(
                            timelineId = state.timelineId,
                            version = state.version,
                            segments = state.segments.map { it.toDto() },
                            totalDurationMs = state.totalDurationMs,
                            totalSegments = state.segmentCount
                        )
                    )
                }

                post("/segments") {
                    val timelineId = call.parameters["timelineId"]!!
                    val req = call.receive<CreateSegmentRequest>()
                    try {
                        val currentVersion = eventStore.getLatestVersion(timelineId)
                        val segment = req.segment.toModel(timelineId, currentVersion + 1)
                        val event = SegmentCreated(
                            timelineId = timelineId,
                            aggregateId = segment.id,
                            version = currentVersion + 1,
                            segment = segment
                        )
                        val eventId = eventStore.append(event, config.kafka.eventsTopic)
                        call.respond(
                            HttpStatusCode.Created,
                            EventAckResponse(eventId, timelineId, event.version, true)
                        )
                    } catch (e: OptimisticLockException) {
                        call.respond(
                            HttpStatusCode.Conflict,
                            ErrorResponse("version_conflict", e.message ?: "Version conflict")
                        )
                    } catch (e: IllegalArgumentException) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ErrorResponse("validation_error", e.message ?: "Invalid request")
                        )
                    }
                }

                put("/segments/{segmentId}") {
                    val timelineId = call.parameters["timelineId"]!!
                    val segmentId = call.parameters["segmentId"]!!
                    val req = call.receive<UpdateSegmentRequest>()

                    val currentVersion = eventStore.getLatestVersion(timelineId)
                    val currentState = projectionRepository.loadTimeline(timelineId)
                    val previousVersion = currentState?.segments?.find { it.id == segmentId }

                    if (previousVersion == null) {
                        call.respond(
                            HttpStatusCode.NotFound,
                            ErrorResponse("not_found", "Segment $segmentId not found")
                        )
                        return@put
                    }

                    if (currentState.version != req.expectedVersion) {
                        call.respond(
                            HttpStatusCode.Conflict,
                            ErrorResponse(
                                "version_conflict",
                                "Expected version ${req.expectedVersion}, current ${currentState.version}"
                            )
                        )
                        return@put
                    }

                    val updated = req.segment.toModel(timelineId, currentVersion + 1)
                    val event = SegmentUpdated(
                        timelineId = timelineId,
                        aggregateId = segmentId,
                        version = currentVersion + 1,
                        previousVersion = previousVersion,
                        segment = updated,
                        expectedVersion = req.expectedVersion
                    )
                    val eventId = eventStore.append(event, config.kafka.eventsTopic)
                    call.respond(EventAckResponse(eventId, timelineId, event.version, true))
                }

                delete("/segments/{segmentId}") {
                    val timelineId = call.parameters["timelineId"]!!
                    val segmentId = call.parameters["segmentId"]!!
                    val req = runCatching { call.receive<DeleteSegmentRequest>() }.getOrNull()
                        ?: DeleteSegmentRequest(expectedVersion = -1)

                    val currentVersion = eventStore.getLatestVersion(timelineId)
                    val currentState = projectionRepository.loadTimeline(timelineId)

                    if (currentState == null || currentState.segments.none { it.id == segmentId }) {
                        call.respond(
                            HttpStatusCode.NotFound,
                            ErrorResponse("not_found", "Segment $segmentId not found")
                        )
                        return@delete
                    }

                    if (req.expectedVersion >= 0 && currentState.version != req.expectedVersion) {
                        call.respond(
                            HttpStatusCode.Conflict,
                            ErrorResponse(
                                "version_conflict",
                                "Expected version ${req.expectedVersion}, current ${currentState.version}"
                            )
                        )
                        return@delete
                    }

                    val event = SegmentDeleted(
                        timelineId = timelineId,
                        aggregateId = segmentId,
                        version = currentVersion + 1,
                        segmentId = segmentId,
                        expectedVersion = currentState.version
                    )
                    val eventId = eventStore.append(event, config.kafka.eventsTopic)
                    call.respond(EventAckResponse(eventId, timelineId, event.version, true))
                }
            }

            post("/api/timelines/{timelineId}/import") {
                val timelineId = call.parameters["timelineId"]!!
                val req = call.receive<BatchImportRequest>()
                val currentVersion = eventStore.getLatestVersion(timelineId)
                val newVersion = currentVersion + 1

                val segments = req.segments.mapIndexed { idx, dto ->
                    dto.copy(orderIndex = idx).toModel(timelineId, newVersion)
                }

                val mode = when (req.mode) {
                    BatchImportRequest.ImportMode.APPEND -> BatchImported.ImportMode.APPEND
                    BatchImportRequest.ImportMode.REPLACE -> BatchImported.ImportMode.REPLACE
                }

                val event = BatchImported(
                    timelineId = timelineId,
                    version = newVersion,
                    segments = segments,
                    mode = mode
                )
                val eventId = eventStore.append(event, config.kafka.eventsTopic)
                call.respond(
                    HttpStatusCode.Accepted,
                    EventAckResponse(eventId, timelineId, newVersion, true)
                )
            }

            post("/api/timelines") {
                val req = runCatching { call.receive<BatchImportRequest>() }.getOrNull()
                val timelineId = java.util.UUID.randomUUID().toString().take(8)
                val segments = req?.segments?.mapIndexed { idx, dto ->
                    dto.copy(orderIndex = idx).toModel(timelineId, 1)
                } ?: emptyList()

                val event = TimelineCreated(
                    timelineId = timelineId,
                    version = 1,
                    initialSegments = segments
                )
                val eventId = eventStore.append(event, config.kafka.eventsTopic)
                call.respond(
                    HttpStatusCode.Created,
                    EventAckResponse(eventId, timelineId, 1, true)
                )
            }

            post("/api/timelines/{timelineId}/reset") {
                val timelineId = call.parameters["timelineId"]!!
                val currentVersion = eventStore.getLatestVersion(timelineId)
                val event = TimelineReset(
                    timelineId = timelineId,
                    version = currentVersion + 1
                )
                val eventId = eventStore.append(event, config.kafka.eventsTopic)
                call.respond(EventAckResponse(eventId, timelineId, event.version, true))
            }
        }
    }
}

private fun StoryboardSegment.toDto(): SegmentDto = SegmentDto(
    id = id,
    orderIndex = orderIndex,
    startTimeMs = startTimeMs,
    endTimeMs = endTimeMs,
    intensity = intensity,
    isReversal = isReversal,
    isKnowledgePoint = isKnowledgePoint,
    isDeclineInducement = isDeclineInducement,
    knowledgePointId = knowledgePointId,
    isOriginal = isOriginal,
    content = content
)
