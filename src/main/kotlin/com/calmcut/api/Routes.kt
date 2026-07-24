package com.calmcut.api

import com.calmcut.api.dto.*
import com.calmcut.domain.StoryboardId
import com.calmcut.domain.events.SegmentData
import com.calmcut.infrastructure.repository.ImportJobStatus
import com.calmcut.infrastructure.repository.StoryboardRepository
import com.calmcut.service.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

fun Route.storyboardRoutes(
    commandService: StoryboardCommandService,
    queryService: StoryboardQueryService,
    importService: StreamingImportService,
    eventProcessor: EventProcessor
) {
    route("/api/v1/storyboards") {
        post {
            val request = call.receive<CreateStoryboardRequest>()
            val result = commandService.createStoryboard(
                externalId = request.externalId,
                title = request.title,
                originalKnowledgePoints = request.originalKnowledgePoints
            )
            when (result) {
                is CommandResult.Success -> call.respond(
                    HttpStatusCode.Created,
                    CommandResponse(
                        success = true,
                        storyboardId = result.storyboardId,
                        version = result.version,
                        eventId = result.eventId
                    )
                )
                is CommandResult.ValidationError -> call.respond(
                    HttpStatusCode.BadRequest,
                    CommandResponse(success = false, validationErrors = result.errors)
                )
                is CommandResult.Error -> call.respond(
                    HttpStatusCode.InternalServerError,
                    CommandResponse(success = false, error = result.message)
                )
                else -> call.respond(HttpStatusCode.InternalServerError, CommandResponse(success = false, error = "Unexpected result"))
            }
        }

        get("/{id}") {
            val id = call.parameters["id"]!!
            val storyboard = queryService.getStoryboard(StoryboardId(id))
            if (storyboard != null) {
                call.respond(storyboard.toResponse())
            } else {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "Storyboard not found"))
            }
        }

        get("/{id}/segments") {
            val id = call.parameters["id"]!!
            val segments = queryService.getSegments(StoryboardId(id))
            call.respond(segments.map { it.toResponse() })
        }

        post("/{id}/segments") {
            val id = call.parameters["id"]!!
            val request = call.receive<AddSegmentRequest>()
            val result = commandService.addSegment(id, request.expectedVersion, request.segment.toSegmentData())
            respondCommandResult(call, result)
        }

        put("/{id}/segments/{segmentId}") {
            val id = call.parameters["id"]!!
            val segmentId = call.parameters["segmentId"]!!
            val request = call.receive<UpdateSegmentRequest>()
            val result = commandService.updateSegment(id, request.expectedVersion, segmentId, request.changes.toChangeSet())
            respondCommandResult(call, result)
        }

        delete("/{id}/segments/{segmentId}") {
            val id = call.parameters["id"]!!
            val segmentId = call.parameters["segmentId"]!!
            val expectedVersion = call.request.queryParameters["expectedVersion"]?.toLongOrNull() ?: 0L
            val result = commandService.deleteSegment(id, expectedVersion, segmentId)
            respondCommandResult(call, result)
        }

        post("/{id}/segments:reorder") {
            val id = call.parameters["id"]!!
            val request = call.receive<ReorderSegmentsRequest>()
            val result = commandService.reorderSegments(id, request.expectedVersion, request.newOrder)
            respondCommandResult(call, result)
        }

        post("/{id}/segments:batchImport") {
            val id = call.parameters["id"]!!
            val request = call.receive<BatchImportRequest>()
            val importJobId = request.importJobId ?: java.util.UUID.randomUUID().toString()
            val result = commandService.batchImportSegments(
                storyboardId = id,
                expectedVersion = request.expectedVersion,
                importJobId = importJobId,
                segments = request.segments.map { it.toSegmentData() }
            )
            respondCommandResult(call, result)
        }

        post("/{id}/import:stream") {
            val id = call.parameters["id"]!!
            val request = call.receive<StreamingImportRequest>()
            val segments = generateLargeSegmentSet(request.totalSegments)
            val flow = importService.createSegmentFlowFromList(segments)
            val jobId = importService.startStreamingImport(StoryboardId(id), request.totalSegments, flow)
            call.respond(HttpStatusCode.Accepted, StreamingImportResponse(jobId = jobId, status = "RUNNING"))
        }

        get("/{id}/import/{jobId}") {
            val jobId = call.parameters["jobId"]!!
            val job = importService.getJobStatus(jobId)
            if (job != null) {
                call.respond(
                    ImportJobStatusResponse(
                        jobId = job.id,
                        storyboardId = job.storyboardId,
                        status = job.status.name,
                        totalSegments = job.totalSegments,
                        processedSegments = job.processedSegments,
                        failedSegments = job.failedSegments,
                        checkpoint = job.checkpoint,
                        startedAt = job.startedAt,
                        completedAt = job.completedAt,
                        cancelledAt = job.cancelledAt,
                        errorMessage = job.errorMessage
                    )
                )
            } else {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "Import job not found"))
            }
        }

        post("/{id}/import/{jobId}:cancel") {
            val jobId = call.parameters["jobId"]!!
            val cancelled = importService.cancelImport(jobId)
            call.respond(mapOf("cancelled" to cancelled))
        }

        get("/{id}/risks") {
            val id = call.parameters["id"]!!
            val result = queryService.getRiskAnalysis(StoryboardId(id))
            if (result != null) {
                call.respond(result.toResponse())
            } else {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "No analysis found. May need to trigger analysis."))
            }
        }

        post("/{id}/risks:rebuild") {
            val id = call.parameters["id"]!!
            val request = call.receiveOrNull<RebuildRequest>() ?: RebuildRequest()
            val result = commandService.requestRebuild(id, request.reason)
            when (result) {
                is CommandResult.Success -> call.respond(
                    HttpStatusCode.Accepted,
                    CommandResponse(success = true, storyboardId = result.storyboardId, version = result.version)
                )
                is CommandResult.NotFound -> call.respond(
                    HttpStatusCode.NotFound,
                    CommandResponse(success = false, error = result.message)
                )
                else -> call.respond(HttpStatusCode.InternalServerError, CommandResponse(success = false, error = "Unexpected error"))
            }
        }

        post("/{id}/risks:rebuildFull") {
            val id = call.parameters["id"]!!
            val result = eventProcessor.rebuildProjectionFromScratch(StoryboardId(id))
            call.respond(result.toResponse())
        }

        post("/{id}/risks:verify") {
            val id = call.parameters["id"]!!
            val currentVersion = queryService.getCurrentVersion(StoryboardId(id)) ?: 0L
            val driftResult = queryService.verifyEquivalence(StoryboardId(id), currentVersion)
            call.respond(
                DriftCheckResponse(
                    hasDrift = driftResult.hasDrift,
                    message = driftResult.message
                )
            )
        }

        get("/{id}/knowledge-points:validate") {
            val id = call.parameters["id"]!!
            val result = queryService.validateKnowledgePoints(StoryboardId(id))
            call.respond(
                KnowledgePointValidationResponse(
                    isValid = result.isValid,
                    originalCount = result.originalCount,
                    revisedCount = result.revisedCount,
                    missingIds = result.missingIds,
                    extraIds = result.extraIds,
                    errors = result.errors
                )
            )
        }
    }

    get("/health") {
        call.respond(mapOf("status" to "UP"))
    }
}

private suspend fun respondCommandResult(call: ApplicationCall, result: CommandResult) {
    when (result) {
        is CommandResult.Success -> call.respond(
            HttpStatusCode.OK,
            CommandResponse(success = true, storyboardId = result.storyboardId, version = result.version, eventId = result.eventId)
        )
        is CommandResult.VersionConflict -> call.respond(
            HttpStatusCode.Conflict,
            CommandResponse(
                success = false,
                error = "Version conflict",
                conflictExpectedVersion = result.expected,
                conflictActualVersion = result.actual
            )
        )
        is CommandResult.ValidationError -> call.respond(
            HttpStatusCode.BadRequest,
            CommandResponse(success = false, validationErrors = result.errors)
        )
        is CommandResult.NotFound -> call.respond(
            HttpStatusCode.NotFound,
            CommandResponse(success = false, error = result.message)
        )
        is CommandResult.Error -> call.respond(
            HttpStatusCode.InternalServerError,
            CommandResponse(success = false, error = result.message)
        )
    }
}

private fun generateLargeSegmentSet(total: Int): List<SegmentData> {
    return (0 until total).map { i ->
        val startMs = i * 3000L
        val intensity = (i % 5) + 1
        val hasReversal = i % 7 == 0 && intensity >= 4
        val isKp = i % 10 == 0
        SegmentData(
            order = i,
            startTimeMs = startMs,
            endTimeMs = startMs + 3000L,
            stimulusIntensity = intensity,
            hasReversal = hasReversal,
            isKnowledgePoint = isKp,
            knowledgePointId = if (isKp) "kp_$i" else null,
            hasScrollInducement = i % 20 == 0,
            contentType = if (isKp) "KNOWLEDGE_POINT" else "CONTENT"
        )
    }
}
