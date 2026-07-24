package com.calmcut.studio.api

import com.calmcut.studio.import.ImportStatus
import com.calmcut.studio.import.StreamingImportService
import com.calmcut.studio.worker.IdempotentProcessor
import com.calmcut.studio.worker.RebuildService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.launch

/**
 * HTTP surface. Write endpoints append versioned events; read endpoints serve
 * the latest analysis projection. Import endpoints drive the streaming importer
 * (durable staging + cancel/resume). Recovery endpoints expose DLQ replay and
 * projection rebuild, both auditable. All analysis output is descriptive of
 * editing structure only and never asserts clinical/medical conclusions.
 */
fun Route.storyboardRoutes(
    writes: StoryboardWriteService,
    repo: com.calmcut.studio.worker.WorkerRepository,
    imports: StreamingImportService,
    processor: IdempotentProcessor,
    rebuilds: RebuildService,
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
            val result = repo.loadAnalysis(id)
                ?: return@get call.respond(HttpStatusCode.NotFound, ErrorResponse("no analysis for storyboard $id"))
            call.respond(AnalysisResponse.from(result))
        }

        // Rebuild the projection from zero by replaying the event log (auditable).
        post("/{id}/rebuild") {
            val id = call.parameters["id"]!!
            val result = rebuilds.rebuild(id)
            call.respond(
                mapOf(
                    "storyboardId" to result.storyboardId,
                    "version" to result.version.toString(),
                    "eventsReplayed" to result.eventsReplayed.toString(),
                    "findings" to result.analysis.findings.size.toString(),
                )
            )
        }

        get("/{id}/audit") {
            val id = call.parameters["id"]!!
            call.respond(processor.auditTrail(id).map(::auditToMap))
        }
    }

    route("/imports") {
        // Kick off a streaming import: stage the segments durably (bounded memory),
        // then run the apply phase. In production the flow is parsed lazily from the
        // streamed request body; here we accept an inline list and stream it.
        post {
            val req = call.receive<BatchImportRequest>()
            val jobId = imports.createJob(req.storyboardId)
            appScope.launch {
                imports.ingest(jobId, req.segments.map { it.toDomain() }.asFlow())
                imports.run(jobId)
            }
            val p = imports.progress(jobId)!!
            call.respond(HttpStatusCode.Accepted, ImportJobResponse(p.jobId, p.storyboardId, p.status.name, p.total, p.processed))
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

        // Actually resume the job from the DB checkpoint against the staged source.
        post("/{jobId}/resume") {
            val jobId = call.parameters["jobId"]!!
            val current = imports.progress(jobId)
                ?: return@post call.respond(HttpStatusCode.NotFound, ErrorResponse("no import job $jobId"))
            if (current.status == ImportStatus.COMPLETED) {
                return@post call.respond(HttpStatusCode.Conflict, ErrorResponse("job $jobId already completed"))
            }
            appScope.launch { imports.resume(jobId) }
            call.respond(HttpStatusCode.Accepted, ImportJobResponse(current.jobId, current.storyboardId, "RESUMING", current.total, current.processed))
        }
    }

    route("/dlq") {
        get {
            call.respond(processor.deadLetters().map {
                mapOf(
                    "eventId" to it.event.eventId,
                    "storyboardId" to it.event.storyboardId,
                    "version" to it.event.version.toString(),
                    "error" to it.error,
                    "attempts" to it.attempts.toString(),
                    "replayed" to it.replayed.toString(),
                    "replayedAt" to (it.replayedAt ?: ""),
                )
            })
        }

        // Replay a dead-lettered event through the normal pipeline (auditable).
        post("/{eventId}/replay") {
            val eventId = call.parameters["eventId"]!!
            val results = processor.replayDeadLetter(eventId)
            if (results.isEmpty()) {
                call.respond(HttpStatusCode.UnprocessableEntity, ErrorResponse("replay produced no result for $eventId"))
            } else {
                call.respond(mapOf("eventId" to eventId, "replayedVersions" to results.map { it.version.toString() }))
            }
        }

        get("/{eventId}/audit") {
            val eventId = call.parameters["eventId"]!!
            call.respond(processor.auditTrail(eventId).map(::auditToMap))
        }
    }
}

private fun auditToMap(a: com.calmcut.studio.worker.AuditEntry): Map<String, String> = mapOf(
    "kind" to a.kind,
    "target" to a.target,
    "detail" to a.detail,
    "outcome" to a.outcome,
    "createdAt" to a.createdAt,
)
