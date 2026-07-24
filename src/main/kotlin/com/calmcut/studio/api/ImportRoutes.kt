package com.calmcut.studio.api

import com.calmcut.studio.import.StreamingImporter
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

class ImportRoutes(
    private val importer: StreamingImporter
) {
    fun register(routing: Routing) {
        with(routing) {
            post("/api/import/jobs") {
                val req = call.receive<ImportJobRequest>()
                val jobId = importer.createJob(req.timelineId, req.totalCount)
                val state = importer.getJobState(jobId)!!
                call.respond(
                    HttpStatusCode.Created,
                    state.toResponse()
                )
            }

            post("/api/import/jobs/{jobId}/chunks") {
                val jobId = call.parameters["jobId"]!!
                val req = call.receive<ImportChunkRequest>()
                val state = importer.getJobState(jobId)
                if (state == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Job not found"))
                    return@post
                }
                if (state.status.name == "CANCELLED") {
                    call.respond(HttpStatusCode.Conflict, ErrorResponse("cancelled", "Job was cancelled"))
                    return@post
                }

                val segments = req.segments.map { it.toModel(state.timelineId, 0L) }
                importer.submitChunk(jobId, req.chunkIndex, segments, req.isLast)
                call.respond(HttpStatusCode.Accepted, mapOf("status" to "accepted", "chunkIndex" to req.chunkIndex))
            }

            get("/api/import/jobs/{jobId}") {
                val jobId = call.parameters["jobId"]!!
                val state = importer.getJobState(jobId)
                if (state == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Job not found"))
                    return@get
                }
                call.respond(state.toResponse())
            }

            post("/api/import/jobs/{jobId}/cancel") {
                val jobId = call.parameters["jobId"]!!
                val cancelled = importer.cancelJob(jobId)
                if (cancelled) {
                    val state = importer.getJobState(jobId)!!
                    call.respond(state.toResponse())
                } else {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("not_found", "Job not found"))
                }
            }

            post("/api/import/jobs/{jobId}/resume") {
                val jobId = call.parameters["jobId"]!!
                val token = importer.resumeJob(jobId)
                if (token == null) {
                    call.respond(
                        HttpStatusCode.Conflict,
                        ErrorResponse("invalid_state", "Job cannot be resumed (not in cancelled/failed state)")
                    )
                    return@post
                }
                val state = importer.getJobState(jobId)!!
                call.respond(state.toResponse())
            }
        }
    }
}

private fun com.calmcut.studio.domain.model.ImportState.toResponse() = ImportJobResponse(
    jobId = jobId,
    timelineId = timelineId,
    status = status.name.lowercase(),
    totalCount = totalCount,
    processedCount = processedCount,
    failedCount = failedCount,
    progress = progress,
    resumeToken = resumeToken,
    errorMessage = errorMessage
)
