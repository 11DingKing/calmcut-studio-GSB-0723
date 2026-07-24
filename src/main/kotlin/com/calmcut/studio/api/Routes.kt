package com.calmcut.studio.api

import com.calmcut.studio.import.ImportStatus
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.worker.DeadLetterSink
import com.calmcut.studio.worker.ProjectionStore
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.asFlow

/**
 * HTTP surface. Write endpoints append versioned events; read endpoints serve
 * the latest analysis projection. Import endpoints drive the streaming importer
 * with cancel/resume. All analysis output is descriptive of editing structure
 * only and never asserts clinical/medical conclusions.
 */
fun Route.storyboardRoutes(
    writes: StoryboardWriteService,
    projections: ProjectionStore,
    imports: StreamingImportService,
    deadLetters: DeadLetterSink,
    appScope: CoroutineScope,
) {
    route("/storyboards") {

        post("/batch-import") {
            val req = call.receive<BatchImportRequest>()
            val event = writes.batchImport(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments") {
            val req = call.receive<CreateSegmentRequest>()
            val event = writes.createSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments/update") {
            val req = call.receive<UpdateSegmentRequest>()
            val event = writes.updateSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        post("/segments/delete") {
            val req = call.receive<DeleteSegmentRequest>()
            val event = writes.deleteSegment(req)
            call.respond(HttpStatusCode.Accepted, EventAckResponse(event.eventId, event.storyboardId, event.version))
        }

        get("/{id}/analysis") {
            val id = call.parameters["id"]!!
            val result = projections.loadAnalysis(id)
                ?: return@get call.respond(HttpStatusCode.NotFound, ErrorResponse("no analysis for storyboard $id"))
            call.respond(AnalysisResponse.from(result))
        }
    }

    route("/imports") {
        // Kick off a streaming import from an inline segment list (large uploads
        // would stream from a file/S3 source; the service consumes any Flow lazily).
        post {
            val req = call.receive<BatchImportRequest>()
            val jobId = imports.createJob(req.storyboardId, req.segments.size.toLong())
            appScope.launch {
                imports.run(jobId, req.segments.map { it.toDomain() }.asFlow())
            }
            val p = imports.progress(jobId)!!
            call.respond(
                HttpStatusCode.Accepted,
                ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed),
            )
        }

        get("/{jobId}") {
            val jobId = call.parameters["jobId"]!!
            val p = imports.progress(jobId)
                ?: return@get call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
            call.respond(ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed))
        }

        post("/{jobId}/cancel") {
            val jobId = call.parameters["jobId"]!!
            val ok = imports.requestCancel(jobId)
            if (ok) call.respond(HttpStatusCode.Accepted, mapOf("cancelRequested" to true))
            else call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
        }

        post("/{jobId}/resume") {
            val jobId = call.parameters["jobId"]!!
            val p = imports.progress(jobId)
                ?: return@post call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
            if (p.status != ImportStatus.CANCELLED) {
                return@post call.respond(HttpStatusCode.Conflict, ErrorResponse("job $jobId is ${p.status}"))
            }
            call.respond(HttpStatusCode.Accepted, ImportJobResponse(p.jobId, p.storyboardId, "RESUMING", p.total, p.processed))
        }
    }

    route("/dlq") {
        get {
            val entries = deadLetters.list().map {
                mapOf(
                    "eventId" to it.event.eventId,
                    "storyboardId" to it.event.storyboardId,
                    "version" to it.event.version.toString(),
                    "error" to it.error,
                    "attempts" to it.attempts.toString(),
                    "replayed" to it.replayed.toString(),
                )
            }
            call.respond(entries)
        }
    }
}
